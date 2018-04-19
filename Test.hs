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
import Parrows.Future
import Parrows.Util

import Control.Arrow

evalTaskInt :: (SendPort (SendPort (Thunk Int)), SendPort Int) -> Process ()
evalTaskInt = evalTaskBase

data MyInt = I {-# NOUNPACK #-} Int deriving (Show, Generic, Typeable)

instance Binary MyInt
instance NFData MyInt where
  rnf (I x) = rnf $ x

evalTaskMyInt :: (SendPort (SendPort (Thunk MyInt)), SendPort MyInt) -> Process ()
evalTaskMyInt = evalTaskBase

-- remotable declaration for all eval tasks
$(remotable ['evalTaskInt, 'evalTaskMyInt])

instance Evaluatable Int where
  evalTask = $(mkClosure 'evalTaskInt)

instance Evaluatable MyInt where
  evalTask = $(mkClosure 'evalTaskMyInt)

myRemoteTable :: RemoteTable
myRemoteTable = Main.__remoteTable initRemoteTable

fib (I n) = I $ go n (0,1)
  where
    go !n (!a, !b) | n==0      = a
                   | otherwise = go (n-1) (b, a+b)

parFib :: Conf -> [MyInt] -> [MyInt]
parFib conf xs = parEvalN conf (repeat fib) $ xs

instance (Evaluatable b, ArrowChoice arr) => ArrowParallel arr a b Conf where
    parEvalN conf fs = evalN fs >>> arr (evalParallel conf) >>> arr runPar

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
      waitForStartup conf

      -- wait a bit
      --threadDelay 1000000
      readMVar (workers conf) >>= print

      print $ parFib conf $ map I [100000..100010]

      -- TODO: actual computation here!
    ["slave", host, port] -> do
      backend <- initializeBackend host port myRemoteTable
      startSlave backend
