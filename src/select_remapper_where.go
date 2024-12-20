package main

import (
	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

type SelectRemapperWhere struct {
	parserWhere *QueryParserWhere
	parserTable *QueryParserTable
	config      *Config
}

func NewSelectRemapperWhere(config *Config) *SelectRemapperWhere {
	return &SelectRemapperWhere{
		parserWhere: NewQueryParserWhere(config),
		parserTable: NewQueryParserTable(config),
		config:      config,
	}
}

func (remapper *SelectRemapperWhere) RemapWhereExpressions(selectStatement *pgQuery.SelectStmt, node *pgQuery.Node, indentLevel int) *pgQuery.SelectStmt {
	if aExpr := node.GetAExpr(); aExpr != nil {
		if aExpr.Lexpr != nil {
			selectStatement = remapper.RemapWhereExpressions(selectStatement, aExpr.Lexpr, indentLevel)
		}
		if aExpr.Rexpr != nil {
			selectStatement = remapper.RemapWhereExpressions(selectStatement, aExpr.Rexpr, indentLevel)
		}
	}

	if funcCall := node.GetFuncCall(); funcCall != nil {
		functionName := funcCall.Funcname[0].GetString_().Sval
		if constant, ok := REMAPPED_CONSTANT_BY_PG_FUNCTION_NAME[functionName]; ok {
			node.Node = pgQuery.MakeAConstStrNode(constant, 0).Node
		}
	}

	return selectStatement
}

func (remapper *SelectRemapperWhere) RemapWhereClauseForTable(qSchemaTable QuerySchemaTable, selectStatement *pgQuery.SelectStmt) *pgQuery.SelectStmt {
	if remapper.parserTable.IsTableFromPgCatalog(qSchemaTable) {
		switch qSchemaTable.Table {
		case PG_TABLE_PG_NAMESPACE:
			// FROM pg_catalog.pg_namespace -> FROM pg_catalog.pg_namespace WHERE nspname != 'main'
			withoutMainSchemaWhereCondition := remapper.parserWhere.MakeExpressionNode("nspname", "!=", "main")
			return remapper.parserWhere.AppendWhereCondition(selectStatement, withoutMainSchemaWhereCondition)
		case PG_TABLE_PG_STATIO_USER_TABLES:
			// FROM pg_catalog.pg_statio_user_tables -> FROM pg_catalog.pg_statio_user_tables WHERE false
			falseWhereCondition := remapper.parserWhere.MakeFalseConditionNode()
			return remapper.parserWhere.OverrideWhereCondition(selectStatement, falseWhereCondition)
		}
	}
	return selectStatement
}
