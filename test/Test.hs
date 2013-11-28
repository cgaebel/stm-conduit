module Main ( main ) where

import Data.List

import Test.Framework (defaultMain, testGroup)
import Test.Framework.Providers.HUnit
import Test.Framework.Providers.QuickCheck2 (testProperty)

import Test.QuickCheck
import Test.HUnit

import qualified Control.Monad as Monad
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Class (lift)
import Control.Concurrent
import Control.Concurrent.STM
import Control.Concurrent.STM.TMQueue
import Data.Conduit
import Data.Conduit.List as CL
import Data.Conduit.Async
import Data.Conduit.TMChan
import Data.Conduit.TQueue

main = defaultMain tests

tests = [
        testGroup "Behaves to spec" [
                  testCase "simpleList using TMChan" test_simpleList
                , testCase "simpleList using TQueue" test_simpleQueue
                , testCase "simpleList using TMQueue" test_simpleMQueue
            ],
        testGroup "Async functions" [
                  testCase "buffer" test_buffer
                , testCase "gatherFrom" test_gatherFrom
            ],
        testGroup "Bug fixes" [
                  testCase "multipleWriters" test_multipleWriters
                , testCase "asyncOperator" test_asyncOperator
            ]
    ]

test_simpleList = do chan <- atomically $ newTMChan
                     forkIO . runResourceT $ sourceList testList $$ sinkTMChan chan
                     lst' <- runResourceT $ sourceTMChan chan $$ consume
                     assertEqual "for the numbers [1..10000]," testList lst'
                     closed <- atomically $ isClosedTMChan chan
                     assertBool "channel is closed after running" closed
    where
        testList = [1..10000]

test_simpleQueue = do q <- atomically $ newTQueue
                      forkIO . runResourceT $ sourceList testList $$ sinkTQueue q
                      lst'  <- runResourceT $ sourceTQueue q $$ CL.take (length testList)
                      assertEqual "for the numbers [1..10000]," testList lst'
    where
        testList = [1..10000]

test_simpleMQueue = do q <- atomically $ newTMQueue
                       forkIO . runResourceT $ sourceList testList $$ sinkTMQueue q
                       lst' <- runResourceT $ sourceTMQueue q $$ consume
                       assertEqual "for the numbers [1..10000]," testList lst'
                       closed <- atomically $ isClosedTMQueue q
                       assertBool "channel is closed after running" closed
    where
        testList = [1..10000]

test_multipleWriters = do ms <- runResourceT $ mergeSources [ sourceList ([1..10]::[Integer])
                                                            , sourceList ([11..20]::[Integer])
                                                            ] 3
                          xs <- runResourceT $ ms $$ consume
                          liftIO $ assertEqual "for the numbers [1..10] and [11..20]," [1..20] $ sort xs

test_asyncOperator = do sum'  <- CL.sourceList [1..n] $$ CL.fold (+) 0
                        assertEqual ("for the sum of 1 to " ++ show n) sum sum'
                        sum'' <- CL.sourceList [1..n] $$& CL.fold (+) 0
                        assertEqual "for the sum computed with the $$ and the $$&" sum' sum''
    where
        n = 100
        sum = n * (n+1) / 2

test_buffer = do
    sum' <- buffer 128 (CL.sourceList [1..100]) (CL.fold (+) 0)
    assertEqual "sum computed using buffer" sum' 5050

test_gatherFrom = do
    sum' <- gatherFrom 128 gen $$ CL.fold (+) 0
    assertEqual "sum computed using gatherFrom" sum' 5050
  where
    gen queue = Monad.void $ Monad.foldM f queue [1..100]
      where
        f q x = do
            atomically $ writeTBQueue q x
            return q
