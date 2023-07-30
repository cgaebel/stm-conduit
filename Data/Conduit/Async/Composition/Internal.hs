{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FunctionalDependencies #-}
-- | 
-- Internal module for concurrent conduit combinators, introduced by Robert J. Macomber.
-- The functions were moved in this module so they can be exposed outside of the library
-- but without any guarantees about API stability.
--
-- See documentation in "Data.Conduit.Async.Composition.Internal" for additional details.
module Data.Conduit.Async.Composition.Internal
  ( CCatable(..)
  , CFConduit(..)
  , CFConduitLike(..)
  , CConduit(..)
  ) where

import Conduit
import Data.Serialize
import System.Directory (removeFile)

-- | Conduits are concatenable; this class describes how.
-- class CCatable (c1 :: * -> * -> (* -> *) -> * -> *) (c2 :: * -> * -> (* -> *) -> * -> *) (c3 :: * -> * -> (* -> *) -> * -> *) | c1 c2 -> c3 where
class CCatable c1 c2 (c3 :: * -> * -> (* -> *) -> * -> *) | c1 c2 -> c3 where
  -- | Concurrently join the producer and consumer, using a bounded queue of the
  -- given size. The producer will block when the queue is full, if it is
  -- producing faster than the consumers is taking from it. Likewise, if the
  -- consumer races ahead, it will block until more input is available.
  --
  -- Exceptions are properly managed and propagated between the two sides, so
  -- the net effect should be equivalent to not using buffer at all, save for
  -- the concurrent interleaving of effects.
  --
  -- This function is similar to '=$='; for one more like '$$', see
  -- 'buffer'.
  --
  -- >>> runCConduit $ buffer' 1 (CL.sourceList [1,2,3]) CL.consume
  -- [1,2,3]
  buffer' :: Int -- ^ Size of the bounded queue in memory
             -> c1 i x m ()
             -> c2 x o m r
             -> c3 i o m r

instance CCatable ConduitT ConduitT CConduit where
  buffer' i a b = buffer' i (Single a) (Single b)

instance CCatable ConduitT CConduit CConduit where
  buffer' i a b = buffer' i (Single a) b

instance CCatable ConduitT CFConduit CFConduit where
  buffer' i a b = buffer' i (asCFConduit a) b

instance CCatable CConduit ConduitT CConduit where
  buffer' i a b = buffer' i a (Single b)

instance CCatable CConduit CConduit CConduit where
  buffer' i (Single a) b = Multiple i a b
  buffer' i (Multiple i' a as) b = Multiple i' a (buffer' i as b)

instance CCatable CConduit CFConduit CFConduit where
  buffer' i a b = buffer' i (asCFConduit a) b

instance CCatable CFConduit ConduitT CFConduit where
  buffer' i a b = buffer' i a (asCFConduit b)

instance CCatable CFConduit CConduit CFConduit where
  buffer' i a b = buffer' i a (asCFConduit b)

instance CCatable CFConduit CFConduit CFConduit where
  buffer' i (FSingle a) b = FMultiple i a b
  buffer' i (FMultiple i' a as) b = FMultiple i' a (buffer' i as b)
  buffer' i (FMultipleF bufsz dsksz tmpDir a as) b = FMultipleF bufsz dsksz tmpDir a (buffer' i as b)

-- | A "concurrent conduit", in which the stages run in parallel with
-- a buffering queue between them.
data CConduit i o m r where
  Single :: ConduitT i o m r -> CConduit i o m r
  Multiple :: Int -> ConduitT i x m () -> CConduit x o m r -> CConduit i o m r

-- | A "concurrent conduit", in which the stages run in parallel with
-- a buffering queue and possibly a disk file between them.
data CFConduit i o m r where
  FSingle :: ConduitT i o m r -> CFConduit i o m r
  FMultiple :: Int -> ConduitT i x m () -> CFConduit x o m r -> CFConduit i o m r
  FMultipleF :: (Serialize x) => Int -> Maybe Int -> FilePath -> ConduitT i x m () -> CFConduit x o m r -> CFConduit i o m r

class CFConduitLike a where
  asCFConduit :: a i o m r -> CFConduit i o m r

instance CFConduitLike ConduitT where
  asCFConduit = FSingle

instance CFConduitLike CConduit where
  asCFConduit (Single c) = FSingle c
  asCFConduit (Multiple i c cs) = FMultiple i c (asCFConduit cs)

instance CFConduitLike CFConduit where
  asCFConduit = id

