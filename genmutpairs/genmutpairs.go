package genmutpairs

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"strings"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/qaqcatz/impomysql/connector"
	"github.com/qaqcatz/impomysql/mutation/stage2"
)

// MutationPair represents a single (DDL, origin SQL, mutated SQL) triple.
type MutationPair struct {
	ID           int    `json:"idx"`
	DDL          string `json:"ddl"`
	OriginSQL    string `json:"origin_sql"`
	MutatedSQL   string `json:"mutate_sql"`
	MutationName string `json:"mutate_name"`
}

// MutationPairResult is the top-level output: mutation_name -> list of pairs (no wrapper).
type MutationPairResult map[string][]*MutationPair

// GenConfig holds the configuration for generating mutation pairs.
type GenConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
	DbName   string `json:"dbname"`
	Count    int    `json:"count"`  // number of pairs per mutation method
	Output   string `json:"output"` // output JSON file path
	Seed     int64  `json:"seed"`
}

// allMutationNames lists all 18 mutation method names.
var allMutationNames = []string{
	stage2.FixMDistinctU,
	stage2.FixMDistinctL,
	stage2.FixMCmpOpU,
	stage2.FixMCmpOpL,
	stage2.FixMUnionAllU,
	stage2.FixMUnionAllL,
	stage2.FixMInNullU,
	stage2.FixMWhere1U,
	stage2.FixMWhere0L,
	stage2.FixMHaving1U,
	stage2.FixMHaving0L,
	stage2.FixMOn1U,
	stage2.FixMOn0L,
	stage2.FixMRmUnionAllL,
	stage2.RdMLikeU,
	stage2.RdMLikeL,
	stage2.RdMRegExpU,
	stage2.RdMRegExpL,
}

// ddlTemplate is a set of DDL statements used to create test tables and insert data.
type ddlTemplate struct {
	Name       string   // table name
	DDLs       []string // CREATE TABLE + INSERT statements
	FullDDLStr string   // joined DDL string for output
	ColNames   []string // column names
}

// buildDDLTemplates generates multiple DDL templates with varying schemas.
func buildDDLTemplates(rng *rand.Rand, count int) []*ddlTemplate {
	templates := make([]*ddlTemplate, 0, count)

	colTypePools := [][]string{
		{"INT", "VARCHAR(50)", "DOUBLE", "TEXT"},
		{"BIGINT", "CHAR(20)", "FLOAT", "VARCHAR(100)"},
		{"INT", "DECIMAL(10,2)", "VARCHAR(30)", "INT"},
		{"SMALLINT", "VARCHAR(20)", "DOUBLE", "INT"},
		{"INT", "TEXT", "INT", "TEXT"},
	}

	colNamePools := [][]string{
		{"id", "name", "val", "city"},
		{"pk", "label", "score", "tag"},
		{"uid", "title", "amount", "info"},
		{"sid", "desc_col", "price", "memo"},
		{"rid", "col_a", "col_b", "col_c"},
	}

	intVals := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}
	strVals := []string{"'A'", "'B'", "'C'", "'D'", "'E'", "'abc'", "'xyz'", "'hello'", "'world'", "'test'"}
	floatVals := []string{"1.5", "2.7", "3.14", "0.99", "10.01", "7.77", "100.0", "0.001", "42.42", "99.9"}

	for i := 0; i < count; i++ {
		cIdx := i % len(colTypePools)
		cnIdx := i % len(colNamePools)

		tableName := fmt.Sprintf("gmp_t%d", i)
		colTypes := colTypePools[cIdx]
		colNames := colNamePools[cnIdx]

		// CREATE TABLE
		var colDefs []string
		for j := 0; j < len(colNames); j++ {
			colDefs = append(colDefs, fmt.Sprintf("%s %s", colNames[j], colTypes[j%len(colTypes)]))
		}
		var keyDefs []string
		for j := 0; j < len(colNames); j++ {
			ct := strings.ToUpper(colTypes[j%len(colTypes)])
			if strings.Contains(ct, "INT") {
				keyDefs = append(keyDefs, fmt.Sprintf("KEY(%s)", colNames[j]))
			}
		}
		allDefs := append(colDefs, keyDefs...)
		createSQL := fmt.Sprintf("CREATE TABLE %s (%s)", tableName, strings.Join(allDefs, ", "))

		// INSERT rows (5~10)
		numRows := 5 + rng.Intn(6)
		var rows []string
		for r := 0; r < numRows; r++ {
			var vals []string
			for j := 0; j < len(colNames); j++ {
				ct := strings.ToUpper(colTypes[j%len(colTypes)])
				switch {
				case strings.Contains(ct, "INT"):
					vals = append(vals, intVals[rng.Intn(len(intVals))])
				case strings.Contains(ct, "FLOAT") || strings.Contains(ct, "DOUBLE") || strings.Contains(ct, "DECIMAL"):
					vals = append(vals, floatVals[rng.Intn(len(floatVals))])
				default:
					vals = append(vals, strVals[rng.Intn(len(strVals))])
				}
			}
			rows = append(rows, "("+strings.Join(vals, ", ")+")")
		}
		insertSQL := fmt.Sprintf("INSERT INTO %s VALUES %s", tableName, strings.Join(rows, ", "))

		ddls := []string{createSQL, insertSQL}
		fullDDL := strings.Join(ddls, ";\n") + ";"

		templates = append(templates, &ddlTemplate{
			Name:       tableName,
			DDLs:       ddls,
			FullDDLStr: fullDDL,
			ColNames:   colNames,
		})
	}

	return templates
}

// generateOriginSQL generates a random origin SQL for the given mutation type.
// Returns the generated SQL string. Each call produces a different SQL by varying
// column selection, values, operators, etc.
func generateOriginSQL(mutName string, tmpl *ddlTemplate, rng *rand.Rand) string {
	t := tmpl.Name
	c := tmpl.ColNames
	intCol := c[0]  // first col is always int
	strCol := c[1]  // second col is string type
	col3 := c[2]
	col4 := c[3]

	rv := func(vals []string) string { return vals[rng.Intn(len(vals))] }
	intVals := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}
	cmpOpsU := []string{">", "<", "="}
	cmpOpsL := []string{">=", "<=", "!="}
	strValsPlain := []string{"A", "B", "C", "D", "E", "abc", "xyz", "hello", "test", "world", "foo", "bar"}

	// LIKE patterns: must NOT be all '%' for RdMLikeU
	likePatterns := []string{
		"A_%", "%B%", "_C%", "h%o", "%est", "te_t", "%llo", "w_r%", "f%o", "b_r",
		"_A_", "%x%z", "a%c", "h_l%", "%or%", "t%st", "_%_", "A%", "%Z", "x_z",
	}
	// LIKE patterns with at least one '%' for RdMLikeL
	likePatternsL := []string{
		"%A%", "%%B%%", "%h%", "%e%", "%x%", "%abc%", "%est%", "%%", "%o%", "%l%",
		"%test%", "%world%", "%foo%", "%bar%", "%z%", "%C%", "%D%", "%E%",
	}
	regexpPatternsU := []string{
		"^[a-z]", "[0-9]$", "[a-z]+", "[0-9]+[a-z]+", "^[A-Z]+$",
		"^abc", "xyz$", "[a-z]?", "^[0-9]+$", "[A-Z]+",
		"[a-z][0-9]+", "^[a-z]+$", "[0-9]?[a-z]",
	}
	regexpPatternsL := []string{
		"[a-z]*", "[0-9]*[a-z]*", ".*", "[A-Z]*",
		"[0-9]*", "[a-z]*[0-9]*", ".*[a-z]*", "[A-Z]*[0-9]*",
	}

	// Randomly choose among several patterns for diversity
	pick := rng.Intn(5) // increase variety with more alternatives

	switch mutName {

	case stage2.FixMDistinctU:
		// Requires DISTINCT = true
		switch pick {
		case 0:
			return fmt.Sprintf("SELECT DISTINCT %s FROM %s", intCol, t)
		case 1:
			return fmt.Sprintf("SELECT DISTINCT %s, %s FROM %s", intCol, strCol, t)
		case 2:
			return fmt.Sprintf("SELECT DISTINCT %s FROM %s WHERE %s %s %s",
				intCol, t, intCol, rv(cmpOpsU), rv(intVals))
		case 3:
			return fmt.Sprintf("SELECT DISTINCT %s, %s, %s FROM %s WHERE %s %s %s",
				intCol, strCol, col3, t, intCol, rv(cmpOpsU), rv(intVals))
		default:
			return fmt.Sprintf("SELECT DISTINCT %s, %s, %s, %s FROM %s",
				intCol, strCol, col3, col4, t)
		}

	case stage2.FixMDistinctL:
		// Requires DISTINCT = false, no ORDER BY, no WITH
		switch pick {
		case 0:
			return fmt.Sprintf("SELECT %s FROM %s", intCol, t)
		case 1:
			return fmt.Sprintf("SELECT %s, %s FROM %s", intCol, strCol, t)
		case 2:
			return fmt.Sprintf("SELECT %s FROM %s WHERE %s %s %s",
				intCol, t, intCol, rv(cmpOpsU), rv(intVals))
		case 3:
			return fmt.Sprintf("SELECT * FROM %s WHERE %s %s %s",
				t, intCol, rv(cmpOpsU), rv(intVals))
		default:
			return fmt.Sprintf("SELECT %s, %s, %s FROM %s",
				intCol, strCol, col3, t)
		}

	case stage2.FixMCmpOpU:
		// a {>|<|=} b
		op := rv(cmpOpsU)
		switch pick {
		case 0:
			return fmt.Sprintf("SELECT * FROM %s WHERE %s %s %s", t, intCol, op, rv(intVals))
		case 1:
			return fmt.Sprintf("SELECT %s FROM %s WHERE %s %s %s", intCol, t, intCol, op, rv(intVals))
		case 2:
			return fmt.Sprintf("SELECT * FROM %s WHERE %s %s %s AND %s %s %s",
				t, intCol, op, rv(intVals), intCol, rv(cmpOpsU), rv(intVals))
		case 3:
			return fmt.Sprintf("SELECT * FROM %s WHERE %s = ANY (SELECT %s FROM %s WHERE %s > %s)",
				t, intCol, intCol, t, intCol, rv(intVals))
		default:
			return fmt.Sprintf("SELECT * FROM %s WHERE %s > ALL (SELECT %s FROM %s WHERE %s < %s)",
				t, intCol, intCol, t, intCol, rv(intVals))
		}

	case stage2.FixMCmpOpL:
		// a {>=|<=|!=} b
		op := rv(cmpOpsL)
		switch pick {
		case 0:
			return fmt.Sprintf("SELECT * FROM %s WHERE %s %s %s", t, intCol, op, rv(intVals))
		case 1:
			return fmt.Sprintf("SELECT %s FROM %s WHERE %s %s %s", intCol, t, intCol, op, rv(intVals))
		case 2:
			return fmt.Sprintf("SELECT * FROM %s WHERE %s %s %s AND %s %s %s",
				t, intCol, op, rv(intVals), intCol, rv(cmpOpsL), rv(intVals))
		case 3:
			return fmt.Sprintf("SELECT %s, %s FROM %s WHERE %s %s %s",
				intCol, strCol, t, intCol, op, rv(intVals))
		default:
			return fmt.Sprintf("SELECT * FROM %s WHERE %s %s %s OR %s %s %s",
				t, intCol, rv(cmpOpsL), rv(intVals), intCol, rv(cmpOpsL), rv(intVals))
		}

	case stage2.FixMUnionAllU:
		// UNION -> UNION ALL
		switch pick {
		case 0:
			return fmt.Sprintf("SELECT %s FROM %s UNION SELECT %s FROM %s", intCol, t, intCol, t)
		case 1:
			return fmt.Sprintf("SELECT %s, %s FROM %s UNION SELECT %s, %s FROM %s WHERE %s > %s",
				intCol, strCol, t, intCol, strCol, t, intCol, rv(intVals))
		case 2:
			return fmt.Sprintf("SELECT * FROM %s WHERE %s > %s UNION SELECT * FROM %s WHERE %s < %s",
				t, intCol, rv(intVals), t, intCol, rv(intVals))
		case 3:
			return fmt.Sprintf("SELECT %s FROM %s WHERE %s = %s UNION SELECT %s FROM %s WHERE %s = %s",
				intCol, t, intCol, rv(intVals), intCol, t, intCol, rv(intVals))
		default:
			return fmt.Sprintf("SELECT %s, %s FROM %s UNION SELECT %s, %s FROM %s",
				intCol, strCol, t, intCol, strCol, t)
		}

	case stage2.FixMUnionAllL:
		// UNION ALL -> UNION
		switch pick {
		case 0:
			return fmt.Sprintf("SELECT %s FROM %s UNION ALL SELECT %s FROM %s", intCol, t, intCol, t)
		case 1:
			return fmt.Sprintf("SELECT %s, %s FROM %s UNION ALL SELECT %s, %s FROM %s WHERE %s > %s",
				intCol, strCol, t, intCol, strCol, t, intCol, rv(intVals))
		case 2:
			return fmt.Sprintf("SELECT * FROM %s WHERE %s > %s UNION ALL SELECT * FROM %s WHERE %s < %s",
				t, intCol, rv(intVals), t, intCol, rv(intVals))
		case 3:
			return fmt.Sprintf("SELECT %s FROM %s WHERE %s = %s UNION ALL SELECT %s FROM %s WHERE %s = %s",
				intCol, t, intCol, rv(intVals), intCol, t, intCol, rv(intVals))
		default:
			return fmt.Sprintf("SELECT %s, %s FROM %s UNION ALL SELECT %s, %s FROM %s",
				intCol, strCol, t, intCol, strCol, t)
		}

	case stage2.FixMInNullU:
		// IN(x,x,x) -> IN(x,x,x,NULL)
		switch pick {
		case 0:
			return fmt.Sprintf("SELECT * FROM %s WHERE %s IN (%s, %s, %s)",
				t, intCol, rv(intVals), rv(intVals), rv(intVals))
		case 1:
			return fmt.Sprintf("SELECT %s FROM %s WHERE %s IN (%s, %s)",
				intCol, t, intCol, rv(intVals), rv(intVals))
		case 2:
			return fmt.Sprintf("SELECT * FROM %s WHERE %s NOT IN (%s, %s, %s)",
				t, intCol, rv(intVals), rv(intVals), rv(intVals))
		case 3:
			return fmt.Sprintf("SELECT %s IN (%s, %s, %s)", rv(intVals), rv(intVals), rv(intVals), rv(intVals))
		default:
			return fmt.Sprintf("SELECT * FROM %s WHERE %s IN (%s, %s, %s, %s)",
				t, intCol, rv(intVals), rv(intVals), rv(intVals), rv(intVals))
		}

	case stage2.FixMWhere1U:
		// WHERE xxx -> WHERE 1
		switch pick {
		case 0:
			return fmt.Sprintf("SELECT * FROM %s WHERE %s > %s", t, intCol, rv(intVals))
		case 1:
			return fmt.Sprintf("SELECT * FROM %s WHERE %s = %s", t, intCol, rv(intVals))
		case 2:
			return fmt.Sprintf("SELECT * FROM %s WHERE TRUE", t)
		case 3:
			return fmt.Sprintf("SELECT %s FROM %s WHERE %s < %s", intCol, t, intCol, rv(intVals))
		default:
			return fmt.Sprintf("SELECT * FROM %s WHERE %s >= %s AND %s <= %s",
				t, intCol, rv(intVals), intCol, rv(intVals))
		}

	case stage2.FixMWhere0L:
		// WHERE xxx -> WHERE 0
		switch pick {
		case 0:
			return fmt.Sprintf("SELECT * FROM %s WHERE %s > %s", t, intCol, rv(intVals))
		case 1:
			return fmt.Sprintf("SELECT * FROM %s WHERE %s = %s", t, intCol, rv(intVals))
		case 2:
			return fmt.Sprintf("SELECT * FROM %s WHERE TRUE", t)
		case 3:
			return fmt.Sprintf("SELECT %s FROM %s WHERE %s < %s", intCol, t, intCol, rv(intVals))
		default:
			return fmt.Sprintf("SELECT * FROM %s WHERE %s >= %s AND %s <= %s",
				t, intCol, rv(intVals), intCol, rv(intVals))
		}

	case stage2.FixMHaving1U:
		// HAVING xxx -> HAVING 1
		switch pick {
		case 0:
			return fmt.Sprintf("SELECT * FROM %s HAVING TRUE", t)
		case 1:
			return fmt.Sprintf("SELECT * FROM %s HAVING %s > %s", t, intCol, rv(intVals))
		case 2:
			return fmt.Sprintf("SELECT %s FROM %s HAVING %s = %s", intCol, t, intCol, rv(intVals))
		case 3:
			return fmt.Sprintf("SELECT * FROM %s HAVING %s < %s", t, intCol, rv(intVals))
		default:
			return fmt.Sprintf("SELECT %s, %s FROM %s HAVING %s >= %s",
				intCol, strCol, t, intCol, rv(intVals))
		}

	case stage2.FixMHaving0L:
		// HAVING xxx -> HAVING 0
		switch pick {
		case 0:
			return fmt.Sprintf("SELECT * FROM %s HAVING TRUE", t)
		case 1:
			return fmt.Sprintf("SELECT * FROM %s HAVING %s > %s", t, intCol, rv(intVals))
		case 2:
			return fmt.Sprintf("SELECT %s FROM %s HAVING %s = %s", intCol, t, intCol, rv(intVals))
		case 3:
			return fmt.Sprintf("SELECT * FROM %s HAVING %s < %s", t, intCol, rv(intVals))
		default:
			return fmt.Sprintf("SELECT %s, %s FROM %s HAVING %s >= %s",
				intCol, strCol, t, intCol, rv(intVals))
		}

	case stage2.FixMOn1U:
		// ON xxx -> ON 1
		op := rv(cmpOpsU)
		switch pick {
		case 0:
			return fmt.Sprintf("SELECT * FROM %s AS a JOIN %s AS b ON a.%s %s b.%s",
				t, t, intCol, op, intCol)
		case 1:
			return fmt.Sprintf("SELECT a.%s, b.%s FROM %s AS a JOIN %s AS b ON a.%s = b.%s",
				intCol, strCol, t, t, intCol, intCol)
		case 2:
			return fmt.Sprintf("SELECT * FROM %s AS a CROSS JOIN %s AS b ON a.%s %s b.%s",
				t, t, intCol, op, intCol)
		case 3:
			return fmt.Sprintf("SELECT * FROM %s AS a INNER JOIN %s AS b ON a.%s %s b.%s",
				t, t, intCol, op, intCol)
		default:
			return fmt.Sprintf("SELECT a.%s FROM %s AS a STRAIGHT_JOIN %s AS b ON a.%s = b.%s",
				intCol, t, t, intCol, intCol)
		}

	case stage2.FixMOn0L:
		// ON xxx -> ON 0
		op := rv(cmpOpsU)
		switch pick {
		case 0:
			return fmt.Sprintf("SELECT * FROM %s AS a JOIN %s AS b ON a.%s %s b.%s",
				t, t, intCol, op, intCol)
		case 1:
			return fmt.Sprintf("SELECT a.%s, b.%s FROM %s AS a JOIN %s AS b ON a.%s = b.%s",
				intCol, strCol, t, t, intCol, intCol)
		case 2:
			return fmt.Sprintf("SELECT * FROM %s AS a CROSS JOIN %s AS b ON a.%s %s b.%s",
				t, t, intCol, op, intCol)
		case 3:
			return fmt.Sprintf("SELECT * FROM %s AS a INNER JOIN %s AS b ON a.%s %s b.%s",
				t, t, intCol, op, intCol)
		default:
			return fmt.Sprintf("SELECT a.%s FROM %s AS a STRAIGHT_JOIN %s AS b ON a.%s = b.%s",
				intCol, t, t, intCol, intCol)
		}

	case stage2.FixMRmUnionAllL:
		// remove Selects[1:] for UNION ALL (needs exactly 2 selects with UNION ALL)
		switch pick {
		case 0:
			return fmt.Sprintf("SELECT %s FROM %s UNION ALL SELECT %s FROM %s", intCol, t, intCol, t)
		case 1:
			return fmt.Sprintf("SELECT * FROM %s UNION ALL SELECT * FROM %s WHERE %s > %s",
				t, t, intCol, rv(intVals))
		case 2:
			return fmt.Sprintf("SELECT %s, %s FROM %s UNION ALL SELECT %s, %s FROM %s",
				intCol, strCol, t, intCol, strCol, t)
		case 3:
			return fmt.Sprintf("SELECT %s FROM %s WHERE %s < %s UNION ALL SELECT %s FROM %s WHERE %s > %s",
				intCol, t, intCol, rv(intVals), intCol, t, intCol, rv(intVals))
		default:
			return fmt.Sprintf("SELECT %s, %s, %s FROM %s UNION ALL SELECT %s, %s, %s FROM %s",
				intCol, strCol, col3, t, intCol, strCol, col3, t)
		}

	case stage2.RdMLikeU:
		// normal char -> '%', '_' -> '%'
		// Requires: both Expr and Pattern are *test_driver.ValueExpr with string
		sv := rv(strValsPlain)
		pat := rv(likePatterns)
		notLike := ""
		if rng.Intn(3) == 0 {
			notLike = " NOT"
		}
		return fmt.Sprintf("SELECT * FROM %s WHERE '%s'%s LIKE '%s'", t, sv, notLike, pat)

	case stage2.RdMLikeL:
		// '%' -> '_'
		sv := rv(strValsPlain)
		pat := rv(likePatternsL)
		notLike := ""
		if rng.Intn(3) == 0 {
			notLike = " NOT"
		}
		return fmt.Sprintf("SELECT * FROM %s WHERE '%s'%s LIKE '%s'", t, sv, notLike, pat)

	case stage2.RdMRegExpU:
		// '^'|'$' -> '', normal char -> '.', '+'|'?' -> '*'
		sv := rv(strValsPlain)
		pat := rv(regexpPatternsU)
		notRegexp := ""
		if rng.Intn(3) == 0 {
			notRegexp = " NOT"
		}
		return fmt.Sprintf("SELECT * FROM %s WHERE '%s'%s REGEXP '%s'", t, sv, notRegexp, pat)

	case stage2.RdMRegExpL:
		// '*' -> '+'|'?'
		sv := rv(strValsPlain)
		pat := rv(regexpPatternsL)
		notRegexp := ""
		if rng.Intn(3) == 0 {
			notRegexp = " NOT"
		}
		return fmt.Sprintf("SELECT * FROM %s WHERE '%s'%s REGEXP '%s'", t, sv, notRegexp, pat)
	}

	return ""
}

// stripCharsetIntroducer removes charset introducer prefixes like _UTF8MB4, _UTF8, _LATIN1
// from SQL strings. TiDB parser's restore may add these prefixes which some MySQL versions
// do not support.
var charsetIntroducerRe = regexp.MustCompile(`(?i)_(?:UTF8MB4|UTF8|LATIN1|BINARY|ASCII|GBK|GB2312|GB18030)\s*'`)

func stripCharsetIntroducer(sql string) string {
	return charsetIntroducerRe.ReplaceAllStringFunc(sql, func(match string) string {
		// Keep only the quote character at the end
		idx := strings.LastIndex(match, "'")
		if idx >= 0 {
			return "'"
		}
		return match
	})
}

// GenerateMutationPairs generates mutation pairs for all 18 methods.
func GenerateMutationPairs(config *GenConfig) (MutationPairResult, error) {
	// Create DB connector
	conn, err := connector.NewConnector(config.Host, config.Port, config.Username, config.Password, config.DbName)
	if err != nil {
		return nil, errors.Wrap(err, "[GenerateMutationPairs]create connector error")
	}

	// Initialize DB
	err = conn.InitDB()
	if err != nil {
		return nil, errors.Wrap(err, "[GenerateMutationPairs]init db error")
	}

	rng := rand.New(rand.NewSource(config.Seed))

	result := make(MutationPairResult)
	for _, name := range allMutationNames {
		result[name] = make([]*MutationPair, 0)
	}

	// Generate DDL templates
	numDDLTemplates := config.Count
	if numDDLTemplates < 20 {
		numDDLTemplates = 20
	}
	ddlTemplates := buildDDLTemplates(rng, numDDLTemplates)

	// Set up all tables in the database
	fmt.Println("[genmutpairs] Setting up tables...")
	for _, tmpl := range ddlTemplates {
		for _, ddl := range tmpl.DDLs {
			r := conn.ExecSQL(ddl)
			if r.Err != nil {
				return nil, errors.Wrap(r.Err, "[GenerateMutationPairs]exec DDL error: "+ddl)
			}
		}
	}
	fmt.Printf("[genmutpairs] %d tables ready.\n", numDDLTemplates)

	globalID := 0

	// For each mutation method, generate the requested number of pairs
	for _, mutName := range allMutationNames {
		fmt.Printf("[genmutpairs] Generating pairs for %-20s (target: %d)...\n", mutName, config.Count)
		needed := config.Count
		attempts := 0
		maxAttempts := needed * 50 // generous upper bound

		debugLogCount := 0 // limit debug output
		for len(result[mutName]) < needed && attempts < maxAttempts {
			attempts++

			// Pick a random DDL template
			ddlIdx := rng.Intn(len(ddlTemplates))
			tmpl := ddlTemplates[ddlIdx]

			// Generate a random origin SQL for this mutation
			originSQL := generateOriginSQL(mutName, tmpl, rng)
			if originSQL == "" {
				continue
			}

			// Try to mutate
			seed := rng.Int63()
			mutateResult := stage2.MutateAll(originSQL, seed)
			if mutateResult.Err != nil {
				if debugLogCount < 3 {
					fmt.Printf("  [debug] MutateAll error: %v | SQL: %s\n", mutateResult.Err, originSQL)
					debugLogCount++
				}
				continue
			}

			// Find matching mutation units
			for _, unit := range mutateResult.MutateUnits {
				if unit.Name != mutName {
					continue
				}
				if unit.Err != nil || unit.Sql == "" {
					if debugLogCount < 3 {
						fmt.Printf("  [debug] unit err: %v | sql empty: %v\n", unit.Err, unit.Sql == "")
						debugLogCount++
					}
					continue
				}

				// Strip charset introducer prefixes (e.g. _UTF8MB4) that TiDB parser
				// may add during restore, which can cause execution errors on some
				// MySQL-compatible databases.
				mutatedSQL := stripCharsetIntroducer(unit.Sql)

				// Execute origin SQL to verify it runs without error
				originResult := conn.ExecSQL(originSQL)
				if originResult.Err != nil {
					if debugLogCount < 3 {
						fmt.Printf("  [debug] origin exec error: %v | SQL: %s\n", originResult.Err, originSQL)
						debugLogCount++
					}
					continue
				}

				// Execute mutated SQL to verify it runs without error
				mutResult := conn.ExecSQL(mutatedSQL)
				if mutResult.Err != nil {
					if debugLogCount < 3 {
						fmt.Printf("  [debug] mutated exec error: %v | SQL: %s\n", mutResult.Err, mutatedSQL)
						debugLogCount++
					}
					continue
				}

				pair := &MutationPair{
					ID:           globalID,
					MutationName: mutName,
					DDL:          tmpl.FullDDLStr,
					OriginSQL:    originSQL,
					MutatedSQL:   mutatedSQL,
				}
				globalID++
				result[mutName] = append(result[mutName], pair)

				if len(result[mutName]) >= needed {
					break
				}
			}
		}

		fmt.Printf("[genmutpairs] %-20s: %d / %d pairs (attempts: %d)\n",
			mutName, len(result[mutName]), needed, attempts)
	}

	return result, nil
}

// Run is the main entry point for the genmutpairs subcommand.
func Run(host string, port int, username, password, dbname string, count int, output string, seed int64) error {
	config := &GenConfig{
		Host:     host,
		Port:     port,
		Username: username,
		Password: password,
		DbName:   dbname,
		Count:    count,
		Output:   output,
		Seed:     seed,
	}

	result, err := GenerateMutationPairs(config)
	if err != nil {
		return errors.Wrap(err, "[Run]generate error")
	}

	// Print summary
	fmt.Println("\n========== Summary ==========")
	totalPairs := 0
	for _, name := range allMutationNames {
		cnt := len(result[name])
		totalPairs += cnt
		fmt.Printf("  %-20s: %d pairs\n", name, cnt)
	}
	fmt.Printf("  %-20s: %d pairs\n", "TOTAL", totalPairs)

	// Write to JSONL file
	dir := filepath.Dir(output)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return errors.Wrap(err, "[Run]create output directory error")
	}
	f, err := os.Create(output)
	if err != nil {
		return errors.Wrap(err, "[Run]create output file error")
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	for _, name := range allMutationNames {
		for _, pair := range result[name] {
			if err := enc.Encode(pair); err != nil {
				return errors.Wrap(err, "[Run]encode jsonl error")
			}
		}
	}


	fmt.Printf("\nOutput written to: %s\n", output)
	return nil
}
