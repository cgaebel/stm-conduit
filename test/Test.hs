module Main ( main ) where

import Data.List

import Test.Framework (defaultMain, testGroup)
import Test.Framework.Providers.HUnit
import Test.Framework.Providers.QuickCheck2 (testProperty)

import Test.QuickCheck
import Test.HUnit

import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Class (lift)
import Control.Concurrent
import Control.Concurrent.STM
import Control.Concurrent.STM.TMQueue
import Data.Conduit
import Data.Conduit.List as CL
import Data.Conduit.TMChan
import Data.Conduit.TQueue

main = defaultMain tests

tests = [
        testGroup "Behaves to spec" [
                  testCase "simpleList using TMChan" test_simpleList
                , testCase "simpleList using TQueue" test_simpleQueue
                , testCase "simpleList using TMQueue" test_simpleMQueue
            ],
        testGroup "Bug fixes" [
                testCase "multipleWriters" test_multipleWriters
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
