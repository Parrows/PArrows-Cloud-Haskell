{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE UndecidableInstances #-}
module Main where

import GHC.Generics (Generic)

import Control.Concurrent(forkIO, threadDelay)
import Control.Concurrent.MVar

import System.Environment (getArgs)

import SimpleLocalNet

import Parrows.Definition
import Parrows.Skeletons.Map

import Parrows.Future hiding (put', get')
import Parrows.Util

import Control.Arrow(arr, ArrowChoice, (>>>))

import Control.DeepSeq

import Control.Distributed.Process.Node(runProcess)
import System.IO.Unsafe(unsafePerformIO)

-- for Sudoku

import Sudoku
import Data.Maybe

evalTaskInt :: (SendPort (SendPort (Thunk Int)), SendPort Int) -> Process ()
evalTaskInt = evalTaskBase

data MyInt = I {-# NOUNPACK #-} Int deriving (Show, Generic, Typeable)

instance Binary MyInt
instance NFData MyInt where
  rnf (I x) = rnf $ x

evalTaskMyInt :: (SendPort (SendPort (Thunk MyInt)), SendPort MyInt) -> Process ()
evalTaskMyInt = evalTaskBase

evalTaskGrid :: (SendPort (SendPort (Thunk (Maybe Grid))), SendPort (Maybe Grid)) -> Process ()
evalTaskGrid = evalTaskBase

evalTaskGridList :: (SendPort (SendPort (Thunk [Maybe Grid])), SendPort [Maybe Grid]) -> Process ()
evalTaskGridList = evalTaskBase

-- remotable declaration for all eval tasks
$(remotable ['evalTaskInt, 'evalTaskMyInt, 'evalTaskGrid, 'evalTaskGridList])

instance Evaluatable Int where
  evalTask = $(mkClosure 'evalTaskInt)

instance Evaluatable MyInt where
  evalTask = $(mkClosure 'evalTaskMyInt)

instance Evaluatable (Maybe Grid) where
  evalTask = $(mkClosure 'evalTaskGrid)

instance Evaluatable [Maybe Grid] where
  evalTask = $(mkClosure 'evalTaskGridList)

myRemoteTable :: RemoteTable
myRemoteTable = Main.__remoteTable initRemoteTable

fib (I n) = I $ go n (0,1)
  where
    go !n (!a, !b) | n==0      = a
                   | otherwise = go (n-1) (b, a+b)

parFib :: Conf -> [MyInt] -> [MyInt]
parFib conf xs = parEvalN conf (repeat fib) $ xs

instance (NFData a, Evaluatable b, ArrowChoice arr) => ArrowParallel arr a b Conf where
    parEvalN conf fs = arr (force) >>> evalN fs >>> arr (evalParallel conf) >>> arr runPar

newtype CloudFuture a = CF (SendPort (SendPort a))

instance NFData (CloudFuture a) where
  rnf _ = ()

{-# NOINLINE ownLocalNode #-}
ownLocalNode :: LocalNode
ownLocalNode = unsafePerformIO $ readMVar ownLocalNodeMVar

{-# NOINLINE ownLocalNodeMVar #-}
ownLocalNodeMVar :: MVar LocalNode
ownLocalNodeMVar = unsafePerformIO $ newEmptyMVar

put' :: (Binary a, Typeable a) => a -> CloudFuture a
put' a = unsafePerformIO $ do
  mvar <- newEmptyMVar
  runProcess ownLocalNode $ do
    (senderSender, senderReceiver) <- newChan

    liftIO $ do
      forkProcess ownLocalNode $ do
        sender <- receiveChan senderReceiver
        sendChan sender a

    liftIO $ putMVar mvar senderSender
  takeMVar mvar >>= (return . CF)

get' :: (Binary a, Typeable a) => CloudFuture a -> a
get' (CF senderSender) = unsafePerformIO $ do
  mvar <- newEmptyMVar
  runProcess ownLocalNode $ do
    (sender, receiver) <- newChan
    sendChan senderSender sender
    a <- receiveChan receiver
    liftIO $ putMVar mvar a
  takeMVar mvar

instance (ArrowChoice arr, ArrowParallel arr a b Conf) => ArrowLoopParallel arr a b Conf where
    loopParEvalN = parEvalN
    postLoopParEvalN _ = evalN

instance (Binary a, Typeable a) => Future CloudFuture a Conf where
    put _ = arr put'
    get _ = arr get'

main :: IO ()
main = do
  args <- getArgs

  case args of
    ["master", host, port] -> do
      backend <- initializeBackend host port myRemoteTable

      localNode <- newLocalNode backend
      putMVar ownLocalNodeMVar localNode

      conf <- defaultInitConf localNode

      -- fork away the master node
      forkIO $ startMaster backend (master conf backend)

      -- wait for startup
      waitForStartup conf

      -- wait a bit
      --threadDelay 1000000
      readMVar (workers conf) >>= print

      grids <- fmap lines $ readFile "sudoku.txt"

      print (length (filter isJust (farm conf 4 solve grids)))

      -- TODO: actual computation here!
    ["slave", host, port] -> do
      backend <- initializeBackend host port myRemoteTable

      localNode <- newLocalNode backend
      putMVar ownLocalNodeMVar localNode

      startSlave backend
