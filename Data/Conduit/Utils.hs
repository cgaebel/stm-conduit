{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE FlexibleInstances #-}
{-| 
This module provide different utility functions that allow to use safe higher 
level usage.

Conduit pairs allow creation of an internal datastructure that acts as a bridge,
and provides input and output conduits. The structure itself is hidden
internally and can't be used directly, this provide an additional
safeness.

In order to create a bridge from your own datastructures you need to do
the following:

  * Make it an instance of 'UnboundedStream' or 'BoundedStream'
  depending on it's properties:

> instance BoundedStream (Proxy2 TBMQueue) TBMQueue where
>  mkBStream _ i = atomically $ newTBMQueue i

  * Add 'IsConduit' instance.

>  instance MonadIO m => IsConduit m TBMQueue where
>    mkSource = sourceTBMQueue
>    mkSink   = flip sinkTBMQueue True

  * Use "pair" or "pairBounded" to create a bridge. Because bridge data structure 
    is hidden and not seen in parameters, we need proxy type to help compiler to 
    choose type, we use "Proxy2" for that.

>  pairTBMQueue = pairBounded (proxy2 :: Proxy2 TBMQueue a)

  * Now we can create a pair of conduits:

> (src, snk) <- pairTBMQueue 32
> Control.Concurrent.Async.concurrently (sender src) (receviver snk)

  As channel is not visible we can close it or send additional messages bypassing
  conduit code.

This package provides predefined pairs for all STM types that are used
in the package. 

-}
module Data.Conduit.Utils
  ( 
  -- * Conduit pairs
  -- ** Low level functions
    pairBounded   -- MonadIO m => m (Source m a, Sink m a ())
  , pair          -- MonadIO m => Int -> m (Source m a, Sink m a ()) 
  -- ** Classes
  , UnboundedStream(..)
  , BoundedStream(..)
  , IsConduit(..)
  -- ** Types
  , Proxy2
  , proxy2        -- Proxy a b
  -- * Specialized functions
  -- $flist
  , pairTQueue    -- MonadIO m => m (Source m a, Sink a m ())
  , pairTMQueue   -- MonadIO m => m (Source m a, Sink a m ())
  , pairTMChan    -- MonadIO m => m (Source m a, Sink a m ())
  , pairTBQueue   -- MonadIO m => Int -> m (Source m a, Sink a m ())
  , pairTBMQueue  -- MonadIO m => Int -> m (Source m a, Sink a m ())
  , pairTBMChan   -- MonadIO m => Int -> m (Source m a, Sink a m ())
  ) where

import Data.Conduit
import Data.Conduit.TMChan
import Data.Conduit.TQueue

import Control.Concurrent.STM
import Control.Concurrent.STM.TMQueue
import Control.Concurrent.STM.TBMQueue

import Control.Monad.IO.Class

-- | Proxy type that can be used to create opaque values.
--
-- This proxy type is required because pair hides internal data structure
-- and proxy is used to help compiler infer internal type.
data Proxy2 (a :: * -> *) b = Proxy2

-- | Construct 'Proxy2' value.
--
-- > (proxy2 :: Proxy2 TChan a)
proxy2 :: Proxy2 a b
proxy2 = Proxy2

-- | Class for structures that can handle unbounded stream of values.
-- Such streams break conduit assumptions that constant memory will be
-- used, because if receiver is slower then sender than values will 
-- be accumulated.
class UnboundedStream i o | i -> o where
  mkUStream :: i a -> IO (o a)

-- | Class for structures that can handle bounded stream of values i.e.
-- there is exists 'Int' value that sets an upper limit on the number 
-- of values that can be handled by structure. Exact meaning of this 
-- limit may depend on the carrier type.
class BoundedStream i o | i -> o where
  mkBStream :: i a -> Int -> IO (o a)

-- | Class that describes how we can make conduit out of the carrier
-- value.
class MonadIO m => IsConduit m (x :: * -> *) where
  mkSink   :: x a -> Sink a m ()
  mkSource :: x a -> Source m a

-- | Create bounded conduit pair, see "BoundedStream" class description.
pairBounded :: (MonadIO m, IsConduit m o, BoundedStream i o)
            => i a    -- ^ Type description.
            -> Int    -- ^ Conduit size.
            -> m (Source m a, Sink a m ())
pairBounded p s = do
  q <- liftIO $ mkBStream p s
  return (mkSource q, mkSink q)

-- | Create unbounded pair, see "UnboundedStream" class description.
pair :: (MonadIO m, IsConduit m o, UnboundedStream i o)
     => i a   -- ^ Type description.
     -> m (Source m a, Sink a m ())
pair p = do
  q <- liftIO $ mkUStream p
  return (mkSource q, mkSink q)

-------------------------------------------------------------------------------
-- Instances
-------------------------------------------------------------------------------
instance BoundedStream (Proxy2 TBQueue) TBQueue where
  mkBStream _ i = atomically $ newTBQueue i

instance BoundedStream (Proxy2 TBMQueue) TBMQueue where
  mkBStream _ i = atomically $ newTBMQueue i

instance BoundedStream (Proxy2 TBMChan) TBMChan where
  mkBStream _ i = atomically $ newTBMChan i

instance UnboundedStream (Proxy2 TMQueue) TMQueue where
  mkUStream _ = atomically $ newTMQueue

instance UnboundedStream (Proxy2 TQueue) TQueue where
  mkUStream _ = atomically $ newTQueue

instance UnboundedStream (Proxy2 TMChan) TMChan where
  mkUStream _ = atomically $ newTMChan

instance MonadIO m => IsConduit m TBQueue where
  mkSource = sourceTBQueue
  mkSink   = sinkTBQueue

instance MonadIO m => IsConduit m TBMQueue where
  mkSource = sourceTBMQueue
  mkSink   = flip sinkTBMQueue True

instance MonadIO m => IsConduit m TMQueue where
  mkSource = sourceTMQueue
  mkSink   = flip sinkTMQueue True

instance MonadIO m => IsConduit m TQueue where
  mkSource = sourceTQueue
  mkSink   = sinkTQueue

instance MonadIO m => IsConduit m TBMChan where
  mkSource = sourceTBMChan
  mkSink   = flip sinkTBMChan True

instance MonadIO m => IsConduit m TMChan where
  mkSource = sourceTMChan
  mkSink   = flip sinkTMChan True

-------------------------------------------------------------------------------
-- Specialized functions
-------------------------------------------------------------------------------
-- $flist
-- List of specialized functions, that can create a bridges over STM types,
-- where *B* stands for bounded *M* stands for closable. If data structure
-- is not closable then there is no way to notify receiver side that bridge
-- is closed, so it's possible to use it only in infinite streams of when
-- some other mechanism of notification is used.
pairTQueue, pairTMQueue, pairTMChan :: MonadIO m => m (Source m a, Sink a m ())
pairTQueue   = pair (proxy2 :: Proxy2 TQueue a)
pairTMQueue  = pair (proxy2 :: Proxy2 TMQueue a)
pairTMChan   = pair (proxy2 :: Proxy2 TMChan a)

pairTBQueue, pairTBMQueue, pairTBMChan :: MonadIO m => Int -> m (Source m a, Sink a m ())
pairTBQueue  = pairBounded (proxy2 :: Proxy2 TBQueue a)
pairTBMQueue = pairBounded (proxy2 :: Proxy2 TBMQueue a)
pairTBMChan  = pairBounded (proxy2 :: Proxy2 TBMChan a)
