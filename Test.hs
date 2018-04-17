{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DeriveDataTypeable  #-}
{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE GADTs     #-}
{-# LANGUAGE KindSignatures     #-}
{-# LANGUAGE Rank2Types     #-}

import System.Environment (getArgs)
import Control.Distributed.Process
import Control.Distributed.Process.Node (initRemoteTable)
import Control.Distributed.Process.Backend.SimpleLocalnet

import Control.Distributed.Process.Closure

import Control.DeepSeq
import Data.Bool
import Data.IORef

import System.IO.Unsafe

import Control.Monad
import Control.Monad.Fix
import Data.Binary
import Data.Typeable

import Control.Concurrent(forkIO, threadDelay)

import Control.Concurrent.MVar

import Debug.Trace

type Conf = State

data State = State {
  workers :: MVar [NodeId],
  shutdown :: MVar Bool,
  started :: MVar Bool
}

initialConf :: IO Conf
initialConf = do
  workersMVar <- newMVar []
  shutdownMVar <- newMVar False
  startedMVar <- newMVar False
  return State { workers = workersMVar, shutdown = shutdownMVar, started = startedMVar }

class (Binary a, Typeable a, NFData a) => Evaluatable a where
  evalTask :: (SendPort (SendPort a), SendPort a) -> Closure (Process ())

evalTaskBase :: (Binary a, Typeable a, NFData a) => 
  (SendPort (SendPort a), SendPort a) -> Process ()
evalTaskBase (inputPipe, output) = do
  (sendMaster, rec) <- newChan
  -- send the master the SendPort, that we
  -- want to listen the other end on for the input
  sendChan inputPipe sendMaster
  a <- receiveChan rec
  sendChan output (rnf a `seq` a)

evalTaskInt :: (SendPort (SendPort Int), SendPort Int) -> Process ()
evalTaskInt = evalTaskBase

$(remotable ['evalTaskInt])

instance Evaluatable Int where
  evalTask = $(mkClosure 'evalTaskInt)

myRemoteTable :: RemoteTable
myRemoteTable = Main.__remoteTable initRemoteTable

forceSingle :: (Evaluatable a) => NodeId -> MVar a -> a -> Process ()
forceSingle node out a = do
  (inputSenderSender, inputSenderReceiver) <- newChan
  (outputSender, outputReceiver) <- newChan

  spawn node (evalTask (inputSenderSender, outputSender))

  inputSender <- receiveChan inputSenderReceiver
  sendChan inputSender a

  forcedA <- receiveChan outputReceiver

  liftIO $ putMVar out a

{-

force :: (Evaluatable a) => Conf -> MVar [a] -> [a] -> Process ()
force conf out as = do
  nodeIds <- return $ unsafePerformIO $ readMVar $ workers conf
  let mvars = repeat (new)

  return ()
  -}

master :: Conf -> Backend -> [NodeId] -> Process ()
master conf backend slaves = do
    forever $ do
      shutdown <- liftIO $ readMVar $ shutdown conf
      if shutdown
        then do
          terminateAllSlaves backend
          die "terminated"
        else do
          slaveProcesses <- findSlaves backend
          let slaveNodes = map processNodeId slaveProcesses
          liftIO $ do
              modifyMVar_ (workers conf) (\old -> return slaveNodes)
              if (length slaveNodes) > 0 then
                modifyMVar_ (started conf) (\_ -> return True)
              else
                return ()

waitUntil condition = fix $ \loop -> do
  cond <- condition
  if cond
    then return ()
    else threadDelay 100 >> loop

hasSlaveNode :: Conf -> IO Bool
hasSlaveNode conf = readMVar (started conf)

main :: IO ()
main = do
  args <- getArgs

  case args of
    ["master", host, port] -> do
      conf <- initialConf
      backend <- initializeBackend host port myRemoteTable

      -- fork away the master node
      forkIO $ startMaster backend (master conf backend)

      -- wait for startup
      waitUntil (hasSlaveNode conf)

      -- wait a bit
      threadDelay 1000000
      readMVar (workers conf) >>= print

      -- TODO: actual computation here!
    ["slave", host, port] -> do
      backend <- initializeBackend host port myRemoteTable
      startSlave backend
