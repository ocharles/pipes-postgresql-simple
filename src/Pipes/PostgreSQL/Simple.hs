-- | Pipes utilities built on top of @postgresql-simple@

module Pipes.PostgreSQL.Simple (
    -- * Querying
    query,
    query_,

    -- * Serialization and Deserialization
    Format(..),
    fromTable,
    toTable
    ) where

import Control.Monad (void)
import Control.Monad.Catch (MonadCatch, catchAll, throwM)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.ByteString (ByteString)
import Data.Int (Int64)
import Data.String (fromString)

import Pipes.PostgreSQL.Simple.SafeT (Format(..))

import qualified Control.Concurrent.Async as Async
import qualified Control.Concurrent.STM as STM
import qualified Data.Text as Text
import qualified Data.Text.Encoding as Text
import qualified Database.PostgreSQL.Simple as Pg
import qualified Database.PostgreSQL.Simple.Copy as Pg
import qualified Pipes
import qualified Pipes.Concurrent as Pipes

--------------------------------------------------------------------------------
-- | Convert a query to a 'Producer' of rows.
--
-- For example,
--
-- > pg <- connectToPostgresql
-- > query pg "SELECT * FROM widgets WHERE ID = ?" (Only widgetId) >-> print
--
-- Will select all widgets for a given @widgetId@, and then print each row to
-- standard output.
query
    :: (MonadIO m, Pg.FromRow r, Pg.ToRow params)
    => Pg.Connection -> Pg.Query -> params -> Pipes.Producer r m ()
query c q p = do
    (o, i, seal) <- liftIO (Pipes.spawn' Pipes.Single)
    worker <- liftIO $ Async.async $ do
        Pg.fold c q p () (const $ void . STM.atomically . Pipes.send o)
        STM.atomically seal
    liftIO $ Async.link worker
    Pipes.fromInput i

-- | Like 'query', but it doesn't perform any query parameter substitution.
query_
    :: (MonadIO m, Pg.FromRow r)
    => Pg.Connection -> Pg.Query -> Pipes.Producer r m ()
query_ c q = do
    (o, i, seal) <- liftIO (Pipes.spawn' Pipes.Single)
    worker <- liftIO $ Async.async $ do
        Pg.fold_ c q () (const $ void . STM.atomically . Pipes.send o)
        STM.atomically seal
    liftIO $ Async.link worker
    Pipes.fromInput i

--------------------------------------------------------------------------------
-- | Convert a table to a byte stream. This is equivilent to a PostgreSQL
-- @COPY ... TO@ statement.
--
-- Returns the number of rows processed.
fromTable
    :: MonadIO m
    => Pg.Connection
    -> Format
    -> String
    -> Pipes.Producer ByteString m Int64
fromTable c fmt tblName = do
    liftIO $ Pg.copy_ c $ fromString $ concat
        [ "COPY ", tblName
        , " TO STDOUT WITH (FORMAT ", show fmt , ")"
        ]
    let go = do
            r <- liftIO (Pg.getCopyData c)
            case r of
                Pg.CopyOutRow bs -> do
                    Pipes.yield bs
                    go
                Pg.CopyOutDone n -> return n
    go
{-# INLINABLE fromTable #-}

--------------------------------------------------------------------------------
-- | Convert a byte stream to a table. This is equivilent to a PostgreSQL
-- @COPY ... FROM@ statement.
--
-- Returns the number of rows processed
toTable
    :: (MonadCatch m, MonadIO m)
    => Pg.Connection
    -> Format
    -> String
    -> Pipes.Producer ByteString m ()
    -> m Int64
toTable c fmt tblName p0 = do
    liftIO $ Pg.copy_ c $ fromString $ concat
        [ "COPY " , tblName
        , " FROM STDIN WITH (FORMAT " , show fmt, "\")"
        ]
    let go p = do
            x <- Pipes.next p `onException`
                   (liftIO . Pg.putCopyError c .
                      Text.encodeUtf8 . Text.pack . show)
            case x of
                Left   ()      -> liftIO (Pg.putCopyEnd c)
                Right (bs, p') -> liftIO (Pg.putCopyData c bs) >> go p'
    go p0

 where

  action `onException` handler =

    action `catchAll` \e -> handler e >> throwM e
{-# INLINABLE toTable #-}
