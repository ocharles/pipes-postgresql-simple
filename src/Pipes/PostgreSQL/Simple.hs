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
import Control.Monad.Trans (lift)
import Data.ByteString (ByteString)
import Data.Int (Int64)

import qualified Control.Concurrent.Async as Async
import qualified Control.Concurrent.STM as STM
import qualified Database.PostgreSQL.Simple as Pg
import qualified Database.PostgreSQL.Simple.Copy as Pg
import qualified Pipes
import qualified Pipes.Concurrent as Pipes

--------------------------------------------------------------------------------
-- | Convert a query to a 'Producer' of rows
query
    :: (Pg.FromRow r, Pg.ToRow params)
    => Pg.Connection -> Pg.Query -> params -> Pipes.Producer r IO ()
query c q p = do
    (o, i, seal) <- lift (Pipes.spawn' Pipes.Single)
    worker <- lift $ Async.async $ do
        Pg.fold c q p () (const $ void . STM.atomically . Pipes.send o)
        STM.atomically seal
    lift $ Async.link worker
    Pipes.fromInput i

-- | The PostgreSQL file format, used by the @COPY@ command
data Format = Text | Binary | CSV

showFmt :: Format -> String
showFmt fmt = case fmt of
    Text   -> "text"
    Binary -> "binary"
    CSV    -> "csv"

--------------------------------------------------------------------------------
-- | Convert a table to a byte stream
--
-- Returns the number of rows processed
fromTable
    :: Pg.Connection
    -> String
    -> Format
    -> Pipes.Producer ByteString IO Int64
fromTable c tblName fmt = do
    lift $ Pg.copy_ c $ fromString $ concat
        [ "COPY "
        , tblName
        , " TO STDOUT WITH (FORMAT \""
        , showFmt fmt
        , "\")"
        ]
    let go = do
            r <- lift (Pg.getCopyData c)
            case r of
                Pg.CopyOutRow bs -> do
                    Pipes.yield bs
                    go
                Pg.CopyOutDone n -> return n
    go
{-# INLINABLE fromTable #-}

--------------------------------------------------------------------------------
-- | Convert a byte stream to a table
--
-- Returns the number of rows processed
toTable
    :: Pg.Connection
    -> String
    -> Format
    -> Pipes.Producer ByteString IO ()
    -> IO Int64
toTable c tblName fmt p0 = do
    Pg.copy_ c $ fromString $ concat
        [ "COPY "
        , tblName
        , " FROM STDIN WITH (FORMAT \""
        , showFmt fmt
        , "\")"
        ]
    let go p = do
            x <- Pipes.next p
            case x of
                Left   ()      -> Pg.putCopyEnd  c
                Right (bs, p') -> Pg.putCopyData c bs       >> go p'
    go p0
{-# INLINABLE toTable #-}
