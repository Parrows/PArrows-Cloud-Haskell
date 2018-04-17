{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DeriveDataTypeable  #-}
{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE GADTs     #-}
{-# LANGUAGE KindSignatures     #-}

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

import Control.Concurrent

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

class (NFData a) => Evaluatable a where
  eval :: a -> Closure (Process a)

tskBase :: NFData a => a -> Process a
tskBase a = (rnf a) `seq` (return a)

tsk :: Int -> Process Int
tsk = tskBase

$(remotable ['tsk])

instance Evaluatable Int where
  eval = $(mkClosure 'tsk)

myRemoteTable :: RemoteTable
myRemoteTable = Main.__remoteTable initRemoteTable

{-
tests :: NT.Transport -> IO ()
tests transport = do
  runProcess localNode (runTasksInPool mvar)
  val <- takeMVar mvar
  putStrLn $ show $ val
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
             modifyMVar_ (started conf) (\_ -> return True)

-- processMain :: Conf -> Process ()
-- processMain

main :: IO ()
main = do
  args <- getArgs

  case args of
    ["master", host, port] -> do
      conf <- initialConf
      backend <- initializeBackend host port myRemoteTable
      forkIO $ startMaster backend (master conf backend)
      forever $ do
        threadDelay 1000000
        readMVar (workers conf) >>= print
        -- putStrLn $ "Conf: " ++ show state
      --
    ["slave", host, port] -> do
      backend <- initializeBackend host port myRemoteTable
      startSlave backend
