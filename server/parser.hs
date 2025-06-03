{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE LambdaCase #-}

module Main where

import Control.Applicative ((<|>), optional)
import qualified Control.Applicative as A
import Control.Monad (void)
import Control.Monad.Combinators.Expr (makeExprParser, Operator(..))
import Data.Aeson (ToJSON(..), FromJSON, encode, decode, Options(..), defaultOptions, genericToJSON, genericParseJSON, fieldLabelModifier, object, (.=), Value(..))
import Data.ByteString.Lazy (ByteString)
import qualified Data.ByteString.Lazy.Char8 as BL
import Data.Char (toLower, isAlphaNum, isSpace)
import Data.List (intercalate)
import Data.Maybe (fromMaybe, isJust, fromJust, catMaybes)
import Data.Void (Void)
import GHC.Generics (Generic)
import Text.Megaparsec
import Text.Megaparsec.Char
import qualified Text.Megaparsec.Char.Lexer as L
import qualified Data.Text as T
import System.Environment (getArgs)
import System.Exit (exitFailure, exitSuccess)
import System.IO (hPutStrLn, stderr)
import qualified Data.Map as Map

type Parser = Parsec Void String

-- AST data types
data SQLStatement 
  = SelectStatement SelectExpr
  | InsertStatement InsertExpr
  | UpdateStatement UpdateExpr
  | DeleteStatement DeleteExpr
  | CreateStatement CreateExpr
  | DropStatement DropExpr
  | AlterStatement AlterExpr
  | ShowStatement ShowExpr
  | VisualizeStatement VisualizeExpr
  | UseStatement String
  | ScriptStatement String
  | BeginTransaction
  | CommitTransaction
  | RollbackTransaction
  | CacheStatement CacheExpr
  deriving (Show, Generic)

data CacheExpr
  = CacheStats
  | CacheClear CacheClearTarget
  deriving (Show, Generic)

data CacheClearTarget
  = CacheClearAll
  | CacheClearTable String
  deriving (Show, Generic)

data SelectExpr = SelectExpr
  { selDistinct :: Bool
    , selColumns :: [SelectColumn]
    , selTables :: [TableExpr]
    , selWhere :: Maybe WhereExpr
    , selOrderBy :: Maybe OrderByExpr
    , selLimit :: Maybe Int
    , selOffset :: Maybe Int
    , selJoinInfo :: Maybe JoinInfo
  } deriving (Show, Generic)

data InsertExpr = InsertExpr
  { insTable :: String
    , insColumns :: [String]
    , insValues :: [[Expr]]
  } deriving (Show, Generic)

data UpdateExpr = UpdateExpr
  { updTable :: String
    , updAssignments :: [(String, Expr)]
    , updWhere :: Maybe WhereExpr
  } deriving (Show, Generic)

data DeleteExpr = DeleteExpr
  { delTable :: String
    , delWhere :: Maybe WhereExpr
  } deriving (Show, Generic)

data CreateExpr
  = CreateTableExpr CreateTableExpr
  | CreateDatabaseExpr String
  | CreateIndexExpr CreateIndexExpr
  | CreateViewExpr CreateViewExpr
  deriving (Show, Generic)

data DropExpr
  = DropTableExpr String
  | DropDatabaseExpr String
  | DropIndexExpr DropIndexExpr
  | DropViewExpr String
  deriving (Show, Generic)

data AlterExpr = AlterExpr
  { altTable :: String
    , altAction :: AlterAction
  } deriving (Show, Generic)

data AlterAction
  = AddColumn ColumnDef
  | DropColumn String
  | ModifyColumn ColumnDef
  deriving (Show, Generic)

data ShowExpr
  = ShowDatabases
  | ShowTables
  | ShowAllTables
  | ShowColumns String
  | ShowIndexes (Maybe String)
  deriving (Show, Generic)

data VisualizeExpr
  = VisualizeBPTree (Maybe String) (Maybe String)
  deriving (Show, Generic)

-- Remove duplicate declarations and define the actual record types
data CreateTableExpr = CreateTableRecord
  { cteName :: String
    , cteColumns :: [ColumnDef]
    , cteConstraints :: [Constraint]
  } deriving (Show, Generic)

data CreateIndexExpr = CreateIndexRecord
  { cieIndexName :: String
    , cieTable :: String
    , cieColumns :: [String]
    , cieUnique :: Bool
  } deriving (Show, Generic)

data CreateViewExpr = CreateViewRecord
  { cveViewName :: String
    , cveQuery :: SelectExpr
  } deriving (Show, Generic)

data DropIndexExpr = DropIndexRecord
  { dieIndexName :: String
    , dieTable :: String
  } deriving (Show, Generic)

data ColumnDef = ColumnDef
  { cdName :: String
    , cdType :: DataType
    , cdConstraints :: [ColumnConstraint]
  } deriving (Show, Generic)

data DataType
  = IntegerType
  | VarcharType (Maybe Int)
  | DecimalType (Maybe (Int, Int))
  | DateTimeType
  | BooleanType
  | StringType
  deriving (Show, Generic)

data ColumnConstraint
  = NotNull
  | PrimaryKey
  | Unique
  | Default Expr
  | AutoIncrement
  | Identity (Maybe (Int, Int))
  | References String String
  deriving (Show, Generic)

data Constraint
  = PKConstraint [String]
  | FKConstraint String [String] String [String]
  | UniqueConstraint [String]
  | CheckConstraint Expr
  deriving (Show, Generic)

data SelectColumn
  = AllColumns
  | TableAllColumns String
  | ExprColumn Expr (Maybe String)
  deriving (Show, Generic)

data TableExpr
  = Table String (Maybe String)
  | TableJoin JoinExpr
  | Subquery SelectExpr String
  deriving (Show, Generic)

data JoinExpr = JoinExpr
  { joinLeft :: TableExpr
    , joinRight :: TableExpr
    , joinType :: JoinType
    , joinCondition :: Maybe Expr
  } deriving (Show, Generic)

data JoinType
  = InnerJoin
  | LeftJoin
  | RightJoin
  | FullJoin
  | CrossJoin
  deriving (Show, Eq, Generic)

data JoinInfo = JoinInfo
  { jiType :: String
    , jiCondition :: Maybe String
    , jiTable1 :: String
    , jiTable2 :: String
    , jiJoinAlgorithm :: Maybe String
  } deriving (Show, Generic)

data WhereExpr
  = Where Expr
  | ParsedWhere ParsedCondition
  deriving (Show, Generic)

data ParsedCondition
  = SimpleCondition String String Expr
  | AndCondition [ParsedCondition]
  | OrCondition [ParsedCondition]
  | NotCondition ParsedCondition
  | RawCondition String
  deriving (Show, Generic)

data OrderByExpr = OrderByExpr
  { orderColumn :: String
    , orderDirection :: SortDirection
  } deriving (Show, Generic)

data SortDirection = Asc | Desc
  deriving (Show, Eq, Generic)

data Expr
  = ColumnRef String (Maybe String)
  | LiteralString String
  | LiteralInt Integer
  | LiteralDecimal Double
  | LiteralNull
  | LiteralBool Bool
  | BinaryOp BinaryOp Expr Expr
  | UnaryOp UnaryOp Expr
  | FunctionCall String [Expr]
  | SubqueryExpr SelectExpr
  | CaseExpr [(Expr, Expr)] (Maybe Expr)
  | ListExpr [Expr]
  deriving (Show, Generic)

data BinaryOp
  = Add | Sub | Mul | Div | Mod
  | Eq | Neq | Lt | Lte | Gt | Gte
  | And | Or
  | Like | NotLike
  | In | NotIn
  | Between | NotBetween
  deriving (Show, Eq, Generic)

data UnaryOp = Not | Neg | Pos | IsNull | IsNotNull
  deriving (Show, Eq, Generic)

-- Parse result type for JSON
data ParseResult
  = Success SQLStatement
  | Error String
  deriving (Show, Generic)

-- JSON instances - simplified automatic derivation
instance ToJSON SQLStatement where
  toJSON = genericToJSON customOptions

instance ToJSON CacheExpr
instance ToJSON CacheClearTarget
instance ToJSON SelectExpr where
  toJSON = genericToJSON customOptions
instance ToJSON InsertExpr where
  toJSON = genericToJSON customOptions
instance ToJSON UpdateExpr where
  toJSON = genericToJSON customOptions
instance ToJSON DeleteExpr where
  toJSON = genericToJSON customOptions
instance ToJSON CreateExpr
instance ToJSON DropExpr
instance ToJSON AlterExpr
instance ToJSON AlterAction
instance ToJSON ShowExpr
instance ToJSON VisualizeExpr
instance ToJSON CreateTableExpr where
  toJSON = genericToJSON customOptions
instance ToJSON CreateIndexExpr where
  toJSON = genericToJSON customOptions
instance ToJSON CreateViewExpr where
  toJSON = genericToJSON customOptions
instance ToJSON DropIndexExpr where
  toJSON = genericToJSON customOptions
instance ToJSON ColumnDef
instance ToJSON DataType
instance ToJSON ColumnConstraint
instance ToJSON Constraint
instance ToJSON SelectColumn
instance ToJSON TableExpr
instance ToJSON JoinExpr
instance ToJSON JoinType
instance ToJSON JoinInfo where
  toJSON = genericToJSON customOptions
instance ToJSON WhereExpr
instance ToJSON ParsedCondition
instance ToJSON OrderByExpr
instance ToJSON SortDirection
instance ToJSON Expr
instance ToJSON BinaryOp
instance ToJSON UnaryOp

instance ToJSON ParseResult where
  toJSON = genericToJSON customOptions

-- Custom JSON options
customOptions :: Options
customOptions = defaultOptions
  { fieldLabelModifier = \s -> case s of
      "selColumns" -> "columns"
      "selTables" -> "tables"
      "selWhere" -> "where"
      "selOrderBy" -> "order_by"
      "selLimit" -> "limit"
      "selOffset" -> "offset"
      "selDistinct" -> "distinct"
      "selJoinInfo" -> "join_info"
      "insTable" -> "table"
      "insColumns" -> "columns"
      "insValues" -> "values"
      "updTable" -> "table"
      "updAssignments" -> "set"
      "updWhere" -> "where"
      "delTable" -> "table"
      "delWhere" -> "where"
      "cteName" -> "table"
      "cteColumns" -> "columns"
      "cteConstraints" -> "constraints"
      "cieIndexName" -> "index_name"
      "cieTable" -> "table"
      "cieColumns" -> "columns"
      "cieUnique" -> "unique"
      "dieIndexName" -> "index"
      "dieTable" -> "table"
      "jiType" -> "type"
      "jiCondition" -> "condition"
      "jiTable1" -> "table1"
      "jiTable2" -> "table2"
      "jiJoinAlgorithm" -> "join_algorithm"
      -- Keep others as is
      _ -> s
  }

-- Lexer
sc :: Parser ()
sc = L.space space1 (L.skipLineComment "--") (L.skipBlockComment "/*" "*/")

lexeme :: Parser a -> Parser a
lexeme = L.lexeme sc

symbol :: String -> Parser String
symbol = L.symbol sc

parens :: Parser a -> Parser a
parens = between (symbol "(") (symbol ")")

identifier :: Parser String
identifier = lexeme $ try $ do
  first <- letterChar <|> char '_'
  rest <- A.many (alphaNumChar <|> char '_')
  let result = first : rest
  -- Check if it's a reserved keyword
  if map toLower result `elem` keywords
    then fail $ "unexpected keyword " ++ result
    else return result

quoted :: Parser String
quoted = lexeme $ do
  char '\''
  str <- A.many (notFollowedBy (char '\'') >> anySingle)
  char '\''
  return str

doubleQuoted :: Parser String
doubleQuoted = lexeme $ do
  char '"'
  str <- A.many (notFollowedBy (char '"') >> anySingle)
  char '"'
  return str

quotedIdentifier :: Parser String
quotedIdentifier = doubleQuoted

anyIdentifier :: Parser String
anyIdentifier = try quotedIdentifier <|> identifier

integer :: Parser Integer
integer = lexeme L.decimal

decimal :: Parser Double
decimal = lexeme L.float

stringLiteral :: Parser String
stringLiteral = quoted <|> doubleQuoted

reservedWord :: String -> Parser ()
reservedWord w = try $ do
  string' w
  notFollowedBy alphaNumChar
  sc

-- Keywords
keywords :: [String]
keywords = ["select", "from", "where", "insert", "into", "values", "update", "set",
            "delete", "create", "table", "index", "drop", "database", "alter", "add",
            "column", "modify", "primary", "key", "foreign", "references", "unique",
            "not", "null", "default", "auto_increment", "identity", "int", "varchar",
            "decimal", "datetime", "boolean", "string", "and", "or", "in", "between",
            "like", "is", "case", "when", "then", "else", "end", "distinct",
            "order", "by", "asc", "desc", "limit", "offset", "join", "inner", "left",
            "right", "full", "cross", "on", "as", "union", "intersect", "except",
            "having", "group", "count", "sum", "avg", "min", "max", "show", "tables",
            "databases", "columns", "indexes", "use", "transaction", "begin", "commit", 
            "rollback", "all_tables", "visualize", "bptree", "cache", "stats", "clear"]

keyword :: String -> Parser String
keyword w = try $ do
  k <- string' w  -- Use string' instead of identifier for keywords
  notFollowedBy alphaNumChar
  sc
  return k
  
kw :: String -> Parser ()
kw w = do
  _ <- keyword w
  return ()

-- Expression parsers
parseExpr :: Parser Expr
parseExpr = makeExprParser parseTerm operatorTable

parseTerm :: Parser Expr
parseTerm = choice
  [ try parseColumnRef      -- Try column references first (including table.column)
  , try parseLiteral
  , try parseFunctionCall
  , try parseSubqueryExpr
  , try parseCaseExpr
  , try parseListExpr
  , parens parseExpr
  ]

parseLiteral :: Parser Expr
parseLiteral = choice
  [ try $ LiteralDecimal <$> decimal
  , LiteralInt <$> integer
  , LiteralString <$> stringLiteral
  , LiteralNull <$ (kw "null")
  , LiteralBool <$> ((True <$ kw "true") <|> (False <$ kw "false"))
  ]

parseColumnRef :: Parser Expr
parseColumnRef = try $ do
  -- First part: could be table name or column name
  first <- anyIdentifier
  -- Check if there's a dot (table.column format)
  maybeDot <- optional $ try $ do
    _ <- symbol "."
    second <- anyIdentifier
    return second
  
  case maybeDot of
    Just column -> return $ ColumnRef column (Just first)  -- table.column
    Nothing -> return $ ColumnRef first Nothing             -- just column

parseFunctionCall :: Parser Expr
parseFunctionCall = try $ do
  name <- identifier
  args <- parens $ parseExpr `sepBy` symbol ","
  return $ FunctionCall name args

parseSubqueryExpr :: Parser Expr
parseSubqueryExpr = try $ do
  _ <- symbol "("
  query <- parseSelect
  _ <- symbol ")"
  return $ SubqueryExpr query

parseCaseExpr :: Parser Expr
parseCaseExpr = try $ do
  _ <- kw "case"
  whenThens <- many1 $ do
    _ <- kw "when"
    whenExpr <- parseExpr
    _ <- kw "then"
    thenExpr <- parseExpr
    return (whenExpr, thenExpr)
  elseExpr <- optional $ do
    _ <- kw "else"
    parseExpr
  _ <- kw "end"
  return $ CaseExpr whenThens elseExpr

parseListExpr :: Parser Expr
parseListExpr = try $ do
  _ <- symbol "("
  exprs <- parseExpr `sepBy1` symbol ","
  _ <- symbol ")"
  return $ ListExpr exprs

operatorTable :: [[Operator Parser Expr]]
operatorTable =
  [ [ Prefix (try $ symbol "+" >> return (UnaryOp Pos))
    , Prefix (try $ symbol "-" >> return (UnaryOp Neg))
    , Prefix (try $ kw "not" >> return (UnaryOp Not))
    ]
  , [ InfixL (try $ symbol "*" >> return (BinaryOp Mul))
    , InfixL (try $ symbol "/" >> return (BinaryOp Div))
    , InfixL (try $ symbol "%" >> return (BinaryOp Mod))
    ]
  , [ InfixL (try $ symbol "+" >> return (BinaryOp Add))
    , InfixL (try $ symbol "-" >> return (BinaryOp Sub))
    ]
  , [ InfixL (try $ symbol ">=" >> return (BinaryOp Gte))
    , InfixL (try $ symbol "<=" >> return (BinaryOp Lte))
    , InfixL (try $ symbol "<>" >> return (BinaryOp Neq))
    , InfixL (try $ symbol "!=" >> return (BinaryOp Neq))
    , InfixL (try $ symbol "=" >> return (BinaryOp Eq))
    , InfixL (try $ symbol ">" >> return (BinaryOp Gt))
    , InfixL (try $ symbol "<" >> return (BinaryOp Lt))
    ]
  , [ InfixL (try $ kw "like" >> return (BinaryOp Like))
    , InfixL (try $ (kw "not" >> kw "like") >> return (BinaryOp NotLike))
    ]
  , [ Postfix (try $ do
        _ <- kw "is"
        _ <- kw "null"
        return (UnaryOp IsNull))
    , Postfix (try $ do
        _ <- kw "is"
        _ <- kw "not"
        _ <- kw "null"
        return (UnaryOp IsNotNull))
    ]
  , [ Postfix (try $ do
        _ <- kw "between"
        lower <- parseExpr
        _ <- kw "and"
        upper <- parseExpr
        return (\expr -> BinaryOp Between expr (ListExpr [lower, upper])))
    , Postfix (try $ do
        _ <- kw "not"
        _ <- kw "between"
        lower <- parseExpr
        _ <- kw "and"
        upper <- parseExpr
        return (\expr -> BinaryOp NotBetween expr (ListExpr [lower, upper])))
    ]
  , [ Postfix (try $ do
        _ <- kw "in"
        list <- choice
          [ ListExpr <$> parens (parseExpr `sepBy` symbol ",")
          , SubqueryExpr <$> parens parseSelect
          ]
        return (\expr -> BinaryOp In expr list))
    , Postfix (try $ do
        _ <- kw "not"
        _ <- kw "in"
        list <- choice
          [ ListExpr <$> parens (parseExpr `sepBy` symbol ",")
          , SubqueryExpr <$> parens parseSelect
          ]
        return (\expr -> BinaryOp NotIn expr list))
    ]
  , [ InfixL (try $ kw "and" >> return (BinaryOp And)) ]
  , [ InfixL (try $ kw "or" >> return (BinaryOp Or)) ]
  ]

-- Remove the old operator helper functions since they're inline now

-- Utility
many1 :: Parser a -> Parser [a]
many1 p = do
  x <- p
  xs <- A.many p
  return (x:xs)

-- Higher-level statement parsers
parseStatement :: Parser SQLStatement
parseStatement = choice
  [ try parseSelect >>= return . SelectStatement
  , try parseInsert >>= return . InsertStatement
  , try parseUpdate >>= return . UpdateStatement
  , try parseDelete >>= return . DeleteStatement
  , try parseCreate >>= return . CreateStatement
  , try parseDrop >>= return . DropStatement
  , try parseAlter >>= return . AlterStatement
  , try parseShow >>= return . ShowStatement
  , try parseVisualize >>= return . VisualizeStatement
  , try parseUse >>= return . UseStatement
  , try parseScript >>= return . ScriptStatement
  , try parseBeginTransaction
  , try parseCommitTransaction
  , try parseRollbackTransaction
  , try parseCache >>= return . CacheStatement
  ]

parseSelect :: Parser SelectExpr
parseSelect = do
  _ <- kw "select"
  distinct <- option False (True <$ kw "distinct")
  columns <- parseSelectColumns
  (tables, joinInfo) <- option ([], Nothing) $ do
    _ <- kw "from"
    parseFromClauseWithJoin
  whereClause <- optional $ do
    _ <- kw "where"
    parseWhereExpr
  orderBy <- optional parseOrderBy
  limit <- optional $ do
    _ <- kw "limit"
    fromIntegral <$> integer
  offset <- optional $ do
    _ <- kw "offset"
    fromIntegral <$> integer
  return $ SelectExpr distinct columns tables whereClause orderBy limit offset joinInfo

parseFromClauseWithJoin :: Parser ([TableExpr], Maybe JoinInfo)
parseFromClauseWithJoin = try parseJoinClause <|> parseRegularTables
  where
    parseJoinClause = do
      leftTable <- parseSimpleTable
      joinType <- parseJoinType
      rightTable <- parseSimpleTable
      condition <- optional $ do
        _ <- kw "on"
        parseExpr
      
      -- Extract table names for JoinInfo
      let leftName = extractTableName leftTable
      let rightName = extractTableName rightTable
      let joinTypeStr = joinTypeToString joinType
      let conditionStr = case condition of
            Just expr -> Just $ exprToString expr
            Nothing -> Nothing
      
      let joinInfo = JoinInfo joinTypeStr conditionStr leftName rightName Nothing
      let joinExpr = TableJoin $ JoinExpr leftTable rightTable joinType condition
      
      return ([joinExpr], Just joinInfo)
    
    parseRegularTables = do
      tables <- parseTableExpr `sepBy` symbol ","
      return (tables, Nothing)

parseTableExpr :: Parser TableExpr
parseTableExpr = choice
  [ try $ do
      _ <- symbol "("
      query <- parseSelect
      _ <- symbol ")"
      alias <- anyIdentifier
      return $ Subquery query alias
  , parseSimpleTable
  ]

parseSelectColumns :: Parser [SelectColumn]
parseSelectColumns = parseSelectColumn `sepBy` symbol ","

parseSelectColumn :: Parser SelectColumn
parseSelectColumn = choice
  [ AllColumns <$ symbol "*"
  , try $ do
      tbl <- identifier
      _ <- symbol "."
      _ <- symbol "*"
      return $ TableAllColumns tbl
  , try $ do
      expr <- parseExpr
      alias <- optional $ do
        _ <- kw "as"
        anyIdentifier
      return $ ExprColumn expr alias
  ]

parseFromClause :: Parser [TableExpr]
parseFromClause = do
  firstTable <- parseSimpleTable
  -- Check if there's a JOIN after the first table
  joinResult <- optional parseJoinSequence
  case joinResult of
    Just (joinType, secondTable, condition) -> do
      -- Create a join expression
      let joinExpr = TableJoin $ JoinExpr firstTable secondTable joinType condition
      return [joinExpr]
    Nothing -> do
      -- No JOIN, check for comma-separated tables
      moreTables <- many $ do
        _ <- symbol ","
        parseSimpleTable
      return (firstTable : moreTables)

parseJoinSequence :: Parser (JoinType, TableExpr, Maybe Expr)
parseJoinSequence = do
  joinType <- parseJoinType
  secondTable <- parseSimpleTable
  condition <- optional $ do
    _ <- kw "on"
    parseExpr
  return (joinType, secondTable, condition)

parseSimpleTable :: Parser TableExpr
parseSimpleTable = try $ do
  name <- anyIdentifier
  alias <- optional $ do
    _ <- optional $ kw "as"
    anyIdentifier
  return $ Table name alias

parseJoinType :: Parser JoinType
parseJoinType = choice
  [ InnerJoin <$ (try (kw "inner" >> kw "join") <|> kw "join")
  , LeftJoin <$ (try (kw "left" >> optional (kw "outer") >> kw "join"))
  , RightJoin <$ (try (kw "right" >> optional (kw "outer") >> kw "join"))
  , FullJoin <$ (try (kw "full" >> optional (kw "outer") >> kw "join"))
  , CrossJoin <$ (try (kw "cross" >> kw "join"))
  ]

parseWhereExpr :: Parser WhereExpr
parseWhereExpr = Where <$> parseExpr

parseOrderBy :: Parser OrderByExpr
parseOrderBy = do
  _ <- kw "order"
  _ <- kw "by"
  col <- anyIdentifier
  dir <- option Asc $ choice
    [ Asc <$ kw "asc"
    , Desc <$ kw "desc"
    ]
  return $ OrderByExpr col dir

parseInsert :: Parser InsertExpr
parseInsert = do
  _ <- kw "insert"
  _ <- kw "into"
  table <- anyIdentifier
  columns <- option [] $ parens $ anyIdentifier `sepBy` symbol ","
  _ <- kw "values"
  values <- parseValuesList
  return $ InsertExpr table columns values

parseValuesList :: Parser [[Expr]]
parseValuesList = valueSet `sepBy` symbol ","
  where
    valueSet = parens $ parseExpr `sepBy` symbol ","

parseUpdate :: Parser UpdateExpr
parseUpdate = do
  _ <- kw "update"
  table <- anyIdentifier
  _ <- kw "set"
  assignments <- parseAssignment `sepBy` symbol ","
  whereClause <- optional $ do
    _ <- kw "where"
    parseWhereExpr
  return $ UpdateExpr table assignments whereClause

parseAssignment :: Parser (String, Expr)
parseAssignment = do
  col <- anyIdentifier
  _ <- symbol "="
  expr <- parseExpr
  return (col, expr)

parseDelete :: Parser DeleteExpr
parseDelete = do
  _ <- kw "delete"
  _ <- kw "from"
  table <- anyIdentifier
  whereClause <- optional $ do
    _ <- kw "where"
    parseWhereExpr
  return $ DeleteExpr table whereClause

parseCreate :: Parser CreateExpr
parseCreate = do
  _ <- kw "create"
  choice
    [ try $ do
        _ <- optional $ kw "table"
        createTable
    , try $ do
        _ <- kw "database"
        name <- anyIdentifier
        return $ CreateDatabaseExpr name
    , try $ do
        unique <- option False (True <$ kw "unique")
        _ <- kw "index"
        name <- anyIdentifier
        _ <- kw "on"
        table <- anyIdentifier
        columns <- parens $ anyIdentifier `sepBy` symbol ","
        return $ CreateIndexExpr $ CreateIndexRecord name table columns unique
    , try $ do
        _ <- kw "view"
        name <- anyIdentifier
        _ <- kw "as"
        query <- parseSelect
        return $ CreateViewExpr $ CreateViewRecord name query
    ]

createTable :: Parser CreateExpr
createTable = do
  name <- anyIdentifier
  _ <- symbol "("
  columns <- parseColumnDef `sepBy` symbol ","
  _ <- symbol ")"
  return $ CreateTableExpr $ CreateTableRecord name columns []

parseColumnDef :: Parser ColumnDef
parseColumnDef = do
  name <- anyIdentifier
  dataType <- parseDataType
  constraints <- A.many parseColumnConstraint
  return $ ColumnDef name dataType constraints

parseDataType :: Parser DataType
parseDataType = choice
  [ IntegerType <$ (kw "int" <|> kw "integer")
  , try $ do
      _ <- kw "varchar"
      size <- optional $ parens $ fromIntegral <$> integer
      return $ VarcharType size
  , try $ do
      _ <- kw "decimal"
      params <- optional $ parens $ do
        p <- fromIntegral <$> integer
        _ <- symbol ","
        s <- fromIntegral <$> integer
        return (p, s)
      return $ DecimalType params
  , DateTimeType <$ kw "datetime"
  , BooleanType <$ kw "boolean"
  , StringType <$ kw "string"
  ]

parseColumnConstraint :: Parser ColumnConstraint
parseColumnConstraint = choice
  [ NotNull <$ (kw "not" >> kw "null")
  , PrimaryKey <$ (kw "primary" >> kw "key")
  , Unique <$ kw "unique"
  , try $ do
      _ <- kw "default"
      Default <$> parseExpr
  , AutoIncrement <$ kw "auto_increment"
  , try $ do
      _ <- kw "identity"
      params <- optional $ parens $ do
        seed <- fromIntegral <$> integer
        _ <- symbol ","
        incr <- fromIntegral <$> integer
        return (seed, incr)
      return $ Identity params
  , try $ do
      _ <- kw "references"
      table <- anyIdentifier
      column <- parens anyIdentifier <|> return "id"
      return $ References table column
  ]

parseDrop :: Parser DropExpr
parseDrop = do
  _ <- kw "drop"
  choice
    [ try $ do
        _ <- kw "table"
        name <- anyIdentifier
        return $ DropTableExpr name
    , try $ do
        _ <- kw "database"
        name <- anyIdentifier
        return $ DropDatabaseExpr name
    , try $ do
        _ <- kw "index"
        name <- anyIdentifier
        _ <- kw "on"
        table <- anyIdentifier
        return $ DropIndexExpr $ DropIndexRecord name table
    , try $ do
        _ <- kw "view"
        name <- anyIdentifier
        return $ DropViewExpr name
    ]

parseAlter :: Parser AlterExpr
parseAlter = do
  _ <- kw "alter"
  _ <- kw "table"
  table <- anyIdentifier
  action <- choice
    [ try $ do
        _ <- kw "add"
        _ <- optional $ kw "column"
        AddColumn <$> parseColumnDef
    , try $ do
        _ <- kw "drop"
        _ <- optional $ kw "column"
        DropColumn <$> anyIdentifier
    , try $ do
        _ <- kw "modify"
        _ <- optional $ kw "column"
        ModifyColumn <$> parseColumnDef
    ]
  return $ AlterExpr table action

parseShow :: Parser ShowExpr
parseShow = do
  _ <- kw "show"
  choice
    [ ShowDatabases <$ kw "databases"
    , try $ ShowAllTables <$ (kw "all_tables" <|> kw "all_table")
    , try $ ShowTables <$ kw "tables"
    , try $ do
        _ <- kw "columns"
        _ <- optional $ kw "from"
        table <- anyIdentifier
        return $ ShowColumns table
    , try $ do
        _ <- kw "indexes" <|> kw "indices"
        table <- optional $ do
          _ <- kw "for"
          anyIdentifier
        return $ ShowIndexes table
    ]

parseVisualize :: Parser VisualizeExpr
parseVisualize = do
  _ <- kw "visualize"
  _ <- kw "bptree"
  indexName <- optional $ try $ do
    name <- anyIdentifier
    _ <- kw "on"
    table <- anyIdentifier
    return (name, Just table)
  table <- if isJust indexName
           then return $ snd $ fromJust indexName
           else optional $ do
             _ <- kw "on"
             anyIdentifier
  let idx = if isJust indexName then Just $ fst $ fromJust indexName else Nothing
  return $ VisualizeBPTree idx table

parseUse :: Parser String
parseUse = do
  _ <- kw "use"
  anyIdentifier

parseScript :: Parser String
parseScript = do
  _ <- kw "script"
  stringLiteral <|> anyIdentifier

parseBeginTransaction :: Parser SQLStatement
parseBeginTransaction = do
  _ <- (kw "begin" <|> kw "start")
  _ <- kw "transaction"
  return BeginTransaction

parseCommitTransaction :: Parser SQLStatement
parseCommitTransaction = do
  _ <- kw "commit"
  _ <- optional $ kw "transaction"
  return CommitTransaction

parseRollbackTransaction :: Parser SQLStatement
parseRollbackTransaction = do
  _ <- kw "rollback"
  _ <- optional $ kw "transaction"
  return RollbackTransaction

parseCache :: Parser CacheExpr
parseCache = do
  _ <- kw "cache"
  choice
    [ CacheStats <$ kw "stats"
    , try $ do
        _ <- kw "clear"
        target <- choice
          [ CacheClearAll <$ kw "all"
          , try $ do
              _ <- kw "table"
              table <- anyIdentifier
              return $ CacheClearTable table
          ]
        return $ CacheClear target
    ]

-- Main parsing function
parseSQLStatement :: String -> Either (ParseErrorBundle String Void) SQLStatement
parseSQLStatement = runParser (sc *> parseStatement <* optional (symbol ";") <* eof) ""

-- Convert parse result to JSON with better error handling
parseResultToJSON :: String -> ByteString
parseResultToJSON input =
  case parseSQLStatement (trim input) of
    Left err -> encode $ Error $ "Parse error: " ++ errorBundlePretty err
    Right stmt -> encode $ Success stmt
  where
    trim = reverse . dropWhile isSpace . reverse . dropWhile isSpace

-- Enhanced main function with better error handling
main :: IO ()
main = do
  args <- getArgs
  case args of
    ["--help"] -> showHelp >> exitSuccess
    ["-h"] -> showHelp >> exitSuccess
    [] -> processStdin -- No arguments, read from stdin
    [sqlString] -> do
      let result = parseResultToJSON sqlString
      BL.putStrLn result
      exitSuccess
    _ -> do
      hPutStrLn stderr "Error: Invalid arguments"
      showHelp
      exitFailure

showHelp :: IO ()
showHelp = putStrLn $ unlines
  [ "HMSSQL Parser - Haskell SQL parser for pyHMSSQL"
  , ""
  , "Usage:"
  , "  hsqlparser                 # Read SQL from stdin, output JSON to stdout"
  , "  hsqlparser \"SQL QUERY\"     # Parse the given SQL query"
  , "  hsqlparser --help          # Show this help message"
  , "  hsqlparser -h              # Show this help message"
  , ""
  , "Examples:"
  , "  hsqlparser \"SELECT * FROM users\""
  , "  echo \"SELECT * FROM users\" | hsqlparser"
  , ""
  , "Output:"
  , "  JSON representation of the parsed SQL statement"
  ]

processStdin :: IO ()
processStdin = do
  input <- getContents
  let result = parseResultToJSON input
  BL.putStrLn result
  exitSuccess

-- Helper functions for table name extraction and join type conversion
extractTableName :: TableExpr -> String
extractTableName (Table name _) = name
extractTableName (TableJoin joinExpr) = extractTableName (joinLeft joinExpr)
extractTableName (Subquery _ alias) = alias

joinTypeToString :: JoinType -> String
joinTypeToString InnerJoin = "INNER"
joinTypeToString LeftJoin = "LEFT"
joinTypeToString RightJoin = "RIGHT"
joinTypeToString FullJoin = "FULL"
joinTypeToString CrossJoin = "CROSS"

exprToString :: Expr -> String
exprToString (BinaryOp Eq (ColumnRef leftCol leftTable) (ColumnRef rightCol rightTable)) =
  let leftFull = maybe leftCol (\t -> t ++ "." ++ leftCol) leftTable
      rightFull = maybe rightCol (\t -> t ++ "." ++ rightCol) rightTable
  in leftFull ++ " = " ++ rightFull
exprToString (BinaryOp And left right) = 
  exprToString left ++ " AND " ++ exprToString right
exprToString (BinaryOp Or left right) = 
  exprToString left ++ " OR " ++ exprToString right
exprToString (BinaryOp Eq (ColumnRef col tbl) (LiteralString val)) =
  let colFull = maybe col (\t -> t ++ "." ++ col) tbl
  in colFull ++ " = '" ++ val ++ "'"
exprToString (BinaryOp Gt (ColumnRef col tbl) (LiteralInt val)) =
  let colFull = maybe col (\t -> t ++ "." ++ col) tbl
  in colFull ++ " > " ++ show val
exprToString (BinaryOp Lt (ColumnRef col tbl) (LiteralInt val)) =
  let colFull = maybe col (\t -> t ++ "." ++ col) tbl
  in colFull ++ " < " ++ show val
exprToString (BinaryOp Gte (ColumnRef col tbl) (LiteralInt val)) =
  let colFull = maybe col (\t -> t ++ "." ++ col) tbl
  in colFull ++ " >= " ++ show val
exprToString (BinaryOp Lte (ColumnRef col tbl) (LiteralInt val)) =
  let colFull = maybe col (\t -> t ++ "." ++ col) tbl
  in colFull ++ " <= " ++ show val
exprToString (ColumnRef col tbl) =
  maybe col (\t -> t ++ "." ++ col) tbl
exprToString (LiteralString s) = "'" ++ s ++ "'"
exprToString (LiteralInt i) = show i
exprToString (LiteralDecimal d) = show d
exprToString expr = show expr -- Fallback for other expressions