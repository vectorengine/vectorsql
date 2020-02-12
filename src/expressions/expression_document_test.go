package expressions

import (
	"os"
	"sort"
	"testing"

	"base/docs"

	"github.com/stretchr/testify/assert"
)

func TestExpressionGenDocs(t *testing.T) {
	var expressionNames []string
	expressionDocs := make(map[string]docs.Documentation)

	for k, v := range unaryExprTable {
		expressionDocs[k] = v(nil).Document()
		expressionNames = append(expressionNames, k)
	}
	for k, v := range binaryExprTable {
		expressionDocs[k] = v(nil, nil).Document()
		expressionNames = append(expressionNames, k)
	}
	for k, v := range scalarExprTable {
		expressionDocs[k] = v(nil).Document()
		expressionNames = append(expressionNames, k)
	}

	sort.Strings(expressionNames)
	var body []docs.Documentation
	for i, name := range expressionNames {
		body = append(body, expressionDocs[name])
		if i != len(expressionNames)-1 {
			body = append(body, docs.Divider())
		}
	}

	f, err := os.Create("../../docs/expressions.md")
	assert.Nil(t, err)
	defer f.Close()

	page := docs.Section(
		"Expression Documentation",
		docs.Body(body...),
	)
	docs.RenderDocumentation(page, f)
}
