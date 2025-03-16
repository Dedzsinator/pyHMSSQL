{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}

module Main where

import Text.Parsec
import Text.Parsec.String (Parser)
import Text.Parsec.Language (haskellStyle)
import qualified Text.Parsec.Token as Token
import Control.Applicative ((<|>), many, optional)
import Data.Aeson (ToJSON, FromJSON, encode, decode)
import GHC.Generics (Generic)
import qualified Data.ByteString.Lazy.Char8 as BL

-- Define SQL AST data types (extended for joins, aggregations, etc.)
data SQLStatement
    = SelectStatement Select
    | InsertStatement Insert
    | UpdateStatement Update
    | DeleteStatement Delete
    | CreateTableStatement CreateTable
    | DropTableStatement DropTable
    | BeginTransaction
    | CommitTransaction
    | RollbackTransaction
    deriving (Show, Generic)

instance ToJSON SQLStatement
instance FromJSON SQLStatement

data Select = Select
    { selectColumns :: [Expression]
    , selectFrom    :: FromClause
    , selectWhere   :: Maybe Expression
    , selectGroupBy :: [Expression]
    , selectHaving  :: Maybe Expression
    } deriving (Show, Generic)

instance ToJSON Select
instance FromJSON Select

data FromClause
    = Table String
    | Join JoinType FromClause FromClause Expression
    deriving (Show, Generic)

instance ToJSON FromClause
instance FromJSON FromClause

data JoinType
    = InnerJoin
    | LeftJoin
    | RightJoin
    | FullJoin
    deriving (Show, Generic)

instance ToJSON JoinType
instance FromJSON JoinType

data Expression
    = Literal String
    | ColumnRef String
    | BinaryOp String Expression Expression
    | Aggregation String Expression
    | Subquery SQLStatement
    deriving (Show, Generic)

instance ToJSON Expression
instance FromJSON Expression

-- Other data types (Insert, Update, Delete, CreateTable, DropTable) remain the same.

-- Parser for SQL statements (extended for joins, aggregations, etc.)
sqlStatement :: Parser SQLStatement
sqlStatement = whiteSpace *> (select <|> insert <|> update <|> delete <|> createTable <|> dropTable <|> begin <|> commit <|> rollback) <* eof

-- SELECT statement parser (extended for joins, aggregations, etc.)
select :: Parser SQLStatement
select = do
    reserved "SELECT"
    columns <- commaSep expression
    reserved "FROM"
    fromClause <- from
    whereClause <- optional where
    groupByClause <- optional (reserved "GROUP" *> reserved "BY" *> commaSep expression)
    havingClause <- optional (reserved "HAVING" *> expression)
    return $ SelectStatement $ Select columns fromClause whereClause (maybe [] id groupByClause) havingClause

-- FROM clause parser (supports joins)
from :: Parser FromClause
from = do
    table <- Table <$> identifier
    joins <- many join
    return $ foldl (\acc (joinType, table', condition) -> Join joinType acc table' condition) table joins

join :: Parser (JoinType, FromClause, Expression)
join = do
    joinType <- choice
        [ reserved "INNER" *> reserved "JOIN" $> InnerJoin
        , reserved "LEFT"  *> reserved "JOIN" $> LeftJoin
        , reserved "RIGHT" *> reserved "JOIN" $> RightJoin
        , reserved "FULL"  *> reserved "JOIN" $> FullJoin
        ]
    table <- Table <$> identifier
    reserved "ON"
    condition <- expression
    return (joinType, table, condition)

-- Other parsers (insert, update, delete, createTable, dropTable) remain the same.

-- Transaction parsers
begin :: Parser SQLStatement
begin = reserved "BEGIN" $> BeginTransaction

commit :: Parser SQLStatement
commit = reserved "COMMIT" $> CommitTransaction

rollback :: Parser SQLStatement
rollback = reserved "ROLLBACK" $> RollbackTransaction

-- Main function to read input, parse, and output JSON
main :: IO ()
main = do
    input <- getLine
    case parse sqlStatement "" input of
        Left err -> BL.putStrLn $ encode ("error" :: String)
        Right stmt -> BL.putStrLn $ encode stmt