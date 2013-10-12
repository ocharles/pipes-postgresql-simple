-- | Pipes utilities built on top of @postgresql-simple@

module Pipes.PostgreSQL.Simple (
    -- * Querying
    query,

    -- * Serialization and Deserialization
    Format(..),
    fromTable,
    toTable
    ) where

import Data.String (fromString)
import Control.Monad (void)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.ByteString (ByteString)
import Data.Int (Int64)

import qualified Control.Concurrent.Async as Async
import qualified Control.Concurrent.STM as STM
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

-- | The PostgreSQL file format, used by the @COPY@ command
data Format = Text | Binary | CSV

showFmt :: Format -> String
showFmt fmt = case fmt of
    Text   -> "text"
    Binary -> "binary"
    CSV    -> "csv"

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
        [ "COPY "
        , tblName
        , " TO STDOUT WITH (FORMAT \""
        , showFmt fmt
        , "\")"
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
    :: MonadIO m
    => Pg.Connection
    -> Format
    -> String
    -> Pipes.Producer ByteString m ()
    -> m Int64
toTable c fmt tblName p0 = do
    liftIO $ Pg.copy_ c $ fromString $ concat
        [ "COPY "
        , tblName
        , " FROM STDIN WITH (FORMAT \""
        , showFmt fmt
        , "\")"
        ]
    let go p = do
            x <- Pipes.next p
            case x of
                Left   ()      -> liftIO (Pg.putCopyEnd c)
                Right (bs, p') -> liftIO (Pg.putCopyData c bs) >> go p'
    go p0
{-# INLINABLE toTable #-}
