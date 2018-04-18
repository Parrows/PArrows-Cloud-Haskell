{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DeriveDataTypeable  #-}
{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE KindSignatures      #-}
{-# LANGUAGE Rank2Types          #-}

import System.Environment (getArgs)
import Control.Distributed.Process
import Control.Distributed.Process.Node (forkProcess, LocalNode, initRemoteTable)
import Control.Distributed.Process.Backend.SimpleLocalnet

import Control.Distributed.Process.Closure

import Control.DeepSeq
import Data.Bool
import Data.IORef

import System.IO.Unsafe
import Data.Array.IO
import System.Random

import Control.Monad
import Control.Monad.Fix
import Data.Binary
import Data.Typeable

import Control.Concurrent(forkIO, threadDelay)

import Control.Concurrent.MVar

import Debug.Trace

type Par a = IO a

runPar :: Par a -> a
runPar = unsafePerformIO

-- | Randomly shuffle a list
--   /O(N)/
-- from: https://wiki.haskell.org/Random_shuffle
shuffle :: [a] -> IO [a]
shuffle xs = do
        ar <- newArray n xs
        forM [1..n] $ \i -> do
            j <- randomRIO (i,n)
            vi <- readArray ar i
            vj <- readArray ar j
            writeArray ar j vi
            return vj
  where
    n = length xs
    newArray :: Int -> [a] -> IO (IOArray Int a)
    newArray n xs =  newListArray (1,n) xs

type Conf = State

data State = State {
  workers :: MVar [NodeId],
  shutdown :: MVar Bool,
  started :: MVar Bool,
  localNode :: LocalNode
}

initialConf :: LocalNode -> IO Conf
initialConf localNode = do
  workersMVar <- newMVar []
  shutdownMVar <- newMVar False
  startedMVar <- newMVar False
  return State {
    workers = workersMVar,
    shutdown = shutdownMVar,
    started = startedMVar,
    localNode = localNode
  }

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

evalSingle :: Evaluatable a => Conf -> NodeId -> a -> Par a
evalSingle conf node a = do
  mvar <- newEmptyMVar
  forkProcess (localNode conf) $ forceSingle node mvar a
  takeMVar mvar

evalParallel :: Evaluatable a => Conf -> [a] -> Par [a]
evalParallel conf as = do
  workers <- readMVar $ workers conf

  -- shuffle the list of workers, so we don't end up spawning
  -- all tasks in the same order everytime
  shuffledWorkers <- shuffle workers

  -- complete the work assignment node to task (NodeId, a)
  let workAssignment = zipWith (,) (cycle shuffledWorkers) as

  -- build the parallel computation with sequence
  sequence $ map (uncurry $ evalSingle conf) workAssignment


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
      backend <- initializeBackend host port myRemoteTable

      localNode <- newLocalNode backend

      conf <- initialConf localNode

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
