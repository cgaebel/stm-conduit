{-# LANGUAGE ScopedTypeVariables, RankNTypes, FlexibleContexts, BangPatterns #-}

module Main ( main ) where

import Data.List (sort)

import Test.Framework (defaultMain, testGroup)
import Test.Framework.Providers.HUnit
import Test.Framework.Providers.QuickCheck2 (testProperty)

import Test.QuickCheck
import Test.HUnit

import qualified Control.Monad as Monad
import Control.Monad.Trans.Resource (runResourceT, allocate, release)
import Control.Concurrent (forkIO, threadDelay)
import Control.Concurrent.STM
import Control.Concurrent.STM.TMQueue
import Data.Conduit
import Data.Conduit.List as CL
import Data.Conduit.Async
import Data.Conduit.TMChan
import Data.Conduit.TQueue
import System.Directory
import Conduit
import qualified UnliftIO as Lifted

main = defaultMain tests

tests = [
        testGroup "Behaves to spec" [
                testCase "simpleList using TMChan" test_simpleList
                , testCase "simpleList using TQueue" test_simpleQueue
                , testCase "simpleList using TMQueue" test_simpleMQueue
            ],
        testGroup "Async functions" [
                  testCase "buffer" test_buffer
                , testCase "multiple buffer" test_multi_buffer
                , testCase "bufferToFile" test_bufferToFile
                , testCase "multiple bufferToFile" test_multi_bufferToFile
                , testCase "mixed buffer" test_mixed_buffer
                , testCase "gatherFrom" test_gatherFrom
                , testCase "drainTo" test_drainTo
                , testCase "mergeConduits" test_mergeConduits
                , testCase "mapConcurrenty" test_mergeConduits
            ],
        testGroup "Bug fixes" [
                  testCase "multipleWriters" test_multipleWriters
                , testCase "asyncOperator" test_asyncOperator
            ]
    ]

test_simpleList :: IO ()
test_simpleList =
   -- We start global resource region because we want to protect
   -- the channel. This is not strictly required under some cases
   -- but is needed in order to have exception safety. We need
   -- to cleanup properly in case of exception, but it depending
   -- on the use case it may be implemented by the other means.
   runResourceT $ do
     (reg, chan) <- allocate (newTMChanIO)
                             (atomically . closeTMChan)
     _ <- Lifted.async $ do
       runConduit $ sourceList testList .| sinkTMChan chan
       -- We have to release resource explicitly in order to
       -- show that no data will be sent here ever.
       release reg
      
     lst' <- runConduit $ sourceTMChan chan .| consume
     liftIO $ do 
       assertEqual "for the numbers [1..10000]," testList lst'
       closed <- atomically $ isClosedTMChan chan
       assertBool "channel is closed after running" closed
    where
        testList = [1..10000]

test_simpleQueue :: IO ()
test_simpleQueue = runResourceT $ do
  q <- liftIO $ atomically $ newTQueue
  liftIO $ forkIO . runConduit $ sourceList testList .| sinkTQueue q
  lst'  <- runConduit $ sourceTQueue q .| CL.take (length testList)
  liftIO $ assertEqual "for the numbers [1..10000]," testList lst'
    where
        testList = [1..10000]

test_simpleMQueue :: IO ()
test_simpleMQueue = runResourceT $ do
  (reg, q) <- allocate (newTMQueueIO) (atomically . closeTMQueue)
  _ <- Lifted.async $ do
        runConduit $ sourceList testList .| sinkTMQueue q
        release reg
        
  lst' <- runConduit $ sourceTMQueue q .| consume
  liftIO $ do
    assertEqual "for the numbers [1..10000]," testList lst'
    closed <- atomically $ isClosedTMQueue q
    assertBool "channel is closed after running" closed
   where
     testList = [1..10000]

test_multipleWriters :: IO ()
test_multipleWriters = do
  xs <- runResourceT $ do
          ms <- mergeSources [ sourceList ([1..10]::[Integer])
                             , sourceList ([11..20]::[Integer])
                             ] 3
          runConduit $ ms .| consume
  assertEqual "for the numbers [1..10] and [11..20]," [1..20] $ sort xs

test_asyncOperator :: IO ()
test_asyncOperator = do sum'  <- runConduit $ CL.sourceList [1..n] .| CL.fold (+) 0
                        assertEqual ("for the sum of 1 to " ++ show n) sum sum'
                        sum'' <- CL.sourceList [1..n] $$& CL.fold (+) 0
                        assertEqual "for the sum computed with the $$ and the $$&" sum' sum''
    where
        n :: Integer
        n = 100
        sum = (n * (n+1)) `div` 2

test_buffer :: IO ()
test_buffer = do
    sum' <- buffer 128 (CL.sourceList [1..100]) (CL.fold (+) 0)
    assertEqual "sum computed using buffer" sum' (5050 :: Integer)

test_multi_buffer :: IO ()
test_multi_buffer = do
    sumDoubles <- CL.sourceList [1..100] $$& mapC (* 2) $=& CL.fold (+) 0
    assertEqual "sum of doubles computed using two buffers" sumDoubles (10100 :: Integer)

-- When we're testing file-buffering, we have to make sure to consume
-- slowly enough to ensure the incoming data piles up enough to be
-- flushed to disk..
slowDown :: (MonadIO m) => Int -> ConduitT x x m ()
slowDown delay = awaitForever $ \x -> do
  liftIO $ threadDelay delay
  yield x

aLot, aLittle :: Int
aLot = 10000
aLittle = 5000

test_bufferToFile :: IO ()
test_bufferToFile = do
    tempDir <- getTemporaryDirectory
    sum' <- runResourceT $ bufferToFile 16 (Just 25) tempDir (CL.sourceList [1 :: Int .. 100]) (slowDown aLittle .| CL.fold (+) 0)
    assertEqual "sum computed using bufferToFile" sum' 5050

test_multi_bufferToFile :: IO ()
test_multi_bufferToFile = do
    tempDir <- getTemporaryDirectory
    sumDoubles <- let buf c = bufferToFile' 16 (Just 25) tempDir c -- "c" avoids monomorphism restriction
                  in runResourceT $ runCConduit $ CL.sourceList [1 :: Int .. 100] `buf` (slowDown aLittle .| mapC (* 2)) `buf` (slowDown aLot .| CL.fold (+) 0)
    assertEqual "sum of doubles computed using bufferToFile" sumDoubles 10100

test_mixed_buffer :: IO ()
test_mixed_buffer = do
    tempDir <- getTemporaryDirectory
    sumDoubles <-
      let buf = bufferToFile' 16 (Just 25) tempDir
      in runResourceT $
           CL.sourceList [1 :: Int .. 100] $$& mapC (* 2) `buf` (slowDown aLittle .| CL.fold (+) 0)
    assertEqual "sum of doubles computed using mixed buffers" sumDoubles 10100
    sumTriples <- let buf = bufferToFile' 16 (Just 25) tempDir
                  in runResourceT $ CL.sourceList [1 :: Int .. 100] `buf` (slowDown aLittle .| mapC (* 3)) $$& CL.fold (+) 0
    assertEqual "sum of triples computed using mixed buffers" sumTriples 15150

test_gatherFrom :: IO ()
test_gatherFrom = do
    sum' <- runConduit $ gatherFrom 128 gen .| CL.fold (+) 0
    assertEqual "sum computed using gatherFrom" sum' 5050
  where
    gen queue = Monad.void $ Monad.foldM f queue [1..100]
      where
        f q x = do
            atomically $ writeTBQueue q x
            return q

test_drainTo :: IO ()
test_drainTo = do
    sum' <- runConduit $ CL.sourceList [1..100] .| drainTo 128 (go 0)
    assertEqual "sum computed using drainTo" sum' 5050
  where
    go !acc queue = do
        mres <- atomically $ readTBQueue queue
        case mres of
            Nothing  -> return acc
            Just res -> go (acc + res) queue

test_mergeConduits :: IO ()
test_mergeConduits = do
    merged <- runResourceT $ mergeConduits
                [ CL.map (* 2)
                , Monad.void $ CL.scan (+) 0
                ] 16
    let
      input = [1..10]
      expected = Prelude.map (2 *) input ++ tail (Prelude.scanl (+) 0 input)
    xs <- runConduit $ sourceList ([1..10] :: [Integer]) .|  merged .| consume
    assertEqual "merged results" (sort expected) (sort xs)

test_mapConcurrently :: IO ()
test_mapConcurrently = do
  let input = [1..100]
  res <- runConduitRes $
    sourceList input
    .| Data.Conduit.Async.mapConcurrently 2 2 2 (return . show)
    .| consume
  assertEqual "mapped result" (Prelude.map show input) res 