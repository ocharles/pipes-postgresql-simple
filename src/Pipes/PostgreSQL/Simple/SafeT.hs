{-# LANGUAGE TypeFamilies #-}
module Pipes.PostgreSQL.Simple.SafeT (Format(..), toTable) where

import Control.Monad (void)
import Control.Monad.Catch (catchAll, throwM)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.ByteString (ByteString)
import Data.String (fromString)

import qualified Database.PostgreSQL.Simple as Pg
import qualified Database.PostgreSQL.Simple.Copy as Pg
import qualified Data.Text as Text
import qualified Data.Text.Encoding as Text
import qualified Pipes
import qualified Pipes.Safe as Pipes

--------------------------------------------------------------------------------
-- | The PostgreSQL file format, used by the @COPY@ command
data Format = Text | Binary | CSV
  deriving (Show)


--------------------------------------------------------------------------------
toTable
    :: (MonadIO m, Pipes.MonadSafe m, Pipes.Base m ~ IO)
    => Pg.Connection
    -> Format
    -> String
    -> Pipes.Consumer ByteString m a
toTable c fmt tblName = do
  putCopyEnd <- Pipes.register (void $ Pg.putCopyEnd c)

  Pipes.liftBase $ Pg.copy_ c $ fromString $ concat
    [ "COPY ", tblName
    , " FROM STDIN WITH (FORMAT " , show fmt, ")"
    ]

  Pipes.for Pipes.cat (liftIO . Pg.putCopyData c)
    `onException` (\e -> do Pipes.release putCopyEnd
                            Pipes.liftBase $ Pg.putCopyError c $
                              Text.encodeUtf8 . Text.pack $ show e)

 where

  action `onException` handler =
    action `catchAll` \e -> handler e >> throwM e

{-# INLINABLE toTable #-}
