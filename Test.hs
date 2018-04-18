{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DeriveDataTypeable  #-}
{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE KindSignatures      #-}
{-# LANGUAGE Rank2Types          #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveGeneric #-}

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

import GHC.Generics (Generic)

import GHC.Packing

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
  localNode :: LocalNode,
  serializeBufferSize :: Int
}

-- | default buffer size used by trySerialize
defaultBufSize :: Int
defaultBufSize = 10 * 2^20 -- 10 MB

defaultInitConf :: LocalNode -> IO Conf
defaultInitConf = initialConf defaultBufSize

initialConf :: Int -> LocalNode -> IO Conf
initialConf serializeBufferSize localNode = do
  workersMVar <- newMVar []
  shutdownMVar <- newMVar False
  startedMVar <- newMVar False
  return State {
    workers = workersMVar,
    shutdown = shutdownMVar,
    started = startedMVar,
    localNode = localNode,
    serializeBufferSize = serializeBufferSize
  }

type Thunk a = Serialized a

class (Binary a, Typeable a, NFData a) => Evaluatable a where
  evalTask :: (SendPort (SendPort (Thunk a)), SendPort a) -> Closure (Process ())

evalTaskBase :: (Binary a, Typeable a, NFData a) => 
  (SendPort (SendPort (Thunk a)), SendPort a) -> Process ()
evalTaskBase (inputPipe, output) = do
  (sendMaster, rec) <- newChan

  -- send the master the SendPort, that we
  -- want to listen the other end on for the input
  sendChan inputPipe sendMaster

  -- receive the actual input
  thunkA <- receiveChan rec

  -- and deserialize
  a <- liftIO $ deserialize thunkA

  -- force the input and send it back to master
  sendChan output (rnf a `seq` a)

evalTaskInt :: (SendPort (SendPort (Thunk Int)), SendPort Int) -> Process ()
evalTaskInt = evalTaskBase

data MyInt = I {-# NOUNPACK #-} Int deriving (Show, Generic, Typeable)

instance Binary MyInt

instance NFData MyInt where
  rnf (I x) = rnf $ x

evalTaskMyInt :: (SendPort (SendPort (Thunk MyInt)), SendPort MyInt) -> Process ()
evalTaskMyInt = evalTaskBase

$(remotable ['evalTaskInt, 'evalTaskMyInt])

instance Evaluatable Int where
  evalTask = $(mkClosure 'evalTaskInt)

instance Evaluatable MyInt where
  evalTask = $(mkClosure 'evalTaskMyInt)

myRemoteTable :: RemoteTable
myRemoteTable = Main.__remoteTable initRemoteTable

forceSingle :: (Evaluatable a) => NodeId -> MVar a -> a -> Process ()
forceSingle node out a = do
  -- create the Channel that we use to send the 
  -- Sender of the input from the slave node from
  (inputSenderSender, inputSenderReceiver) <- newChan

  -- create the channel to receive the output from
  (outputSender, outputReceiver) <- newChan

  -- spawn the actual evaluation task on the given node
  -- and pass the two sender objects we created above
  spawn node (evalTask (inputSenderSender, outputSender))

  -- wait for the slave to send the input sender
  inputSender <- receiveChan inputSenderReceiver

  thunkA <- liftIO $ trySerialize a

  -- send the input to the slave
  sendChan inputSender thunkA

  -- TODO: do optional timeout variant so that the computation
  -- runs on the master machine instead
  -- so that we can guarantee results

  -- wait for the result from the slave
  forcedA <- receiveChan outputReceiver

  -- put the output back into the passed MVar
  liftIO $ putMVar out forcedA

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
              modifyMVar_ (workers conf) (\_ -> return slaveNodes)
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

fib (I n) = I $ go n (0,1)
  where
    go !n (!a, !b) | n==0      = a
                   | otherwise = go (n-1) (b, a+b)

parFib :: Conf -> [MyInt] -> [MyInt]
parFib conf xs = runPar $ evalParallel conf $ map fib xs

main :: IO ()
main = do
  args <- getArgs

  case args of
    ["master", host, port] -> do
      backend <- initializeBackend host port myRemoteTable

      localNode <- newLocalNode backend

      conf <- defaultInitConf localNode

      -- fork away the master node
      forkIO $ startMaster backend (master conf backend)

      -- wait for startup
      waitUntil (hasSlaveNode conf)

      -- wait a bit
      --threadDelay 1000000
      readMVar (workers conf) >>= print

      print $ parFib conf $ map I [100000..100010]

      -- TODO: actual computation here!
    ["slave", host, port] -> do
      backend <- initializeBackend host port myRemoteTable
      startSlave backend
