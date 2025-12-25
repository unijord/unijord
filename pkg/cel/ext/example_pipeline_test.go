package ext_test

import (
	"fmt"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/unijord/unijord/pkg/cel/ext"
)

func Example_pipeline() {
	env, err := cel.NewEnv(
		append(ext.AllFuncs(),
			cel.Variable("_event_id", cel.StringType),
			cel.Variable("_event_type", cel.StringType),
			cel.Variable("_occurred_at", cel.TimestampType),
			cel.Variable("_ingested_at", cel.TimestampType),
			cel.Variable("_schema_id", cel.IntType),
			cel.Variable("_lsn", cel.IntType),
			cel.Variable("event", cel.MapType(cel.StringType, cel.DynType)),
			cel.Variable("email_normalized", cel.StringType),
			cel.Variable("order_total", cel.DoubleType),
		)...,
	)
	if err != nil {
		panic(err)
	}

	transforms := []struct {
		field string
		expr  string
	}{
		{"email_normalized", `lower(trim(event.email))`},
		{"phone_clean", `regexReplace(event.phone, '[^0-9]', '')`},

		{"event_date", `date(_occurred_at)`},
		{"event_year", `year(_occurred_at)`},
		{"event_month", `month(_occurred_at)`},

		{"currency", `orDefault(event.currency, 'USD')`},
		{"discount", `ifNull(event.discount_percent, 0.0)`},

		{"user_hash", `xxhash(event.user_id)`},
		{"email_hash", `sha256(email_normalized)`},
	}

	compiled := make(map[string]cel.Program)
	for _, t := range transforms {
		ast, issues := env.Compile(t.expr)
		if issues != nil && issues.Err() != nil {
			panic(fmt.Sprintf("compile %s: %v", t.field, issues.Err()))
		}
		prog, err := env.Program(ast)
		if err != nil {
			panic(err)
		}
		compiled[t.field] = prog
	}

	dropIfExpr := `isNull(event.user_id) || event.is_test == true`
	dropIfAST, _ := env.Compile(dropIfExpr)
	dropIfProg, _ := env.Program(dropIfAST)

	keepIfExpr := `order_total > 0.0`
	keepIfAST, _ := env.Compile(keepIfExpr)
	keepIfProg, _ := env.Program(keepIfAST)

	occurredAt := time.Date(2024, 6, 15, 14, 30, 45, 0, time.UTC)
	ingestedAt := time.Date(2024, 6, 15, 14, 30, 46, 0, time.UTC)

	inputEvent := map[string]any{
		"_event_id":    "evt_abc123",
		"_event_type":  "order.created",
		"_occurred_at": occurredAt,
		"_ingested_at": ingestedAt,
		"_schema_id":   int64(1),
		"_lsn":         int64(12345),
		"event": map[string]any{
			"order_id":         "ORD-98765",
			"user_id":          "usr_12345",
			"email":            "  John.Doe@Example.COM  ",
			"phone":            "+1 (555) 123-4567",
			"currency":         nil,
			"discount_percent": nil,
			"is_test":          false,
			"credit_card":      "4111-1111-1111-1111",
			"ssn":              "123-45-6789",
			"items": []any{
				map[string]any{"sku": "WIDGET-A", "price": 29.99, "quantity": int64(2)},
				map[string]any{"sku": "WIDGET-B", "price": 49.99, "quantity": int64(1)},
				map[string]any{"sku": "WIDGET-C", "price": 19.99, "quantity": int64(3)},
			},
		},
	}

	output := make(map[string]any)
	for k, v := range inputEvent {
		output[k] = v
	}

	items := inputEvent["event"].(map[string]any)["items"].([]any)
	var orderTotal float64
	for _, item := range items {
		m := item.(map[string]any)
		orderTotal += m["price"].(float64) * float64(m["quantity"].(int64))
	}
	output["order_total"] = orderTotal

	for _, t := range transforms {
		activation := map[string]any{}
		for k, v := range output {
			activation[k] = v
		}

		result, _, err := compiled[t.field].Eval(activation)
		if err != nil {
			fmt.Printf("Error evaluating %s: %v\n", t.field, err)
			continue
		}
		output[t.field] = result.Value()
	}

	filterActivation := map[string]any{}
	for k, v := range output {
		filterActivation[k] = v
	}

	dropResult, _, _ := dropIfProg.Eval(filterActivation)
	if dropResult.Value().(bool) {
		fmt.Println("Event DROPPED by drop_if filter")
		return
	}

	keepResult, _, _ := keepIfProg.Eval(filterActivation)
	if !keepResult.Value().(bool) {
		fmt.Println("Event DROPPED by keep_if filter")
		return
	}

	eventMap := output["event"].(map[string]any)
	eventMap["credit_card"] = "**REDACTED**"
	eventMap["ssn"] = "**REDACTED**"

	fmt.Printf("email_normalized: %v\n", output["email_normalized"])
	fmt.Printf("phone_clean: %v\n", output["phone_clean"])
	fmt.Printf("event_date: %v\n", output["event_date"])
	fmt.Printf("event_year: %v\n", output["event_year"])
	fmt.Printf("event_month: %v\n", output["event_month"])
	fmt.Printf("order_total: %.2f\n", output["order_total"])
	fmt.Printf("currency: %v\n", output["currency"])
	fmt.Printf("discount: %v\n", output["discount"])
	fmt.Printf("user_hash: %v\n", output["user_hash"])
	fmt.Printf("email_hash: %v\n", output["email_hash"].(string)[:16]+"...")
	fmt.Printf("credit_card: %v\n", eventMap["credit_card"])
	fmt.Printf("ssn: %v\n", eventMap["ssn"])

	// Output:
	// email_normalized: john.doe@example.com
	// phone_clean: 15551234567
	// event_date: 2024-06-15
	// event_year: 2024
	// event_month: 6
	// order_total: 169.94
	// currency: USD
	// discount: 0
	// user_hash: a81a7b2d595168a1
	// email_hash: 836f82db99121b34...
	// credit_card: **REDACTED**
	// ssn: **REDACTED**
}

// Example_filterDropped shows events that would be dropped by filters.
func Example_filterDropped() {
	env, _ := cel.NewEnv(
		append(ext.AllFuncs(),
			cel.Variable("event", cel.MapType(cel.StringType, cel.DynType)),
			cel.Variable("order_total", cel.DoubleType),
		)...,
	)

	dropIfAST, _ := env.Compile(`isNull(event.user_id) || event.is_test == true`)
	dropIfProg, _ := env.Program(dropIfAST)

	keepIfAST, _ := env.Compile(`order_total > 0.0`)
	keepIfProg, _ := env.Program(keepIfAST)

	testCases := []struct {
		name   string
		event  map[string]any
		total  float64
		reason string
	}{
		{
			name:   "null_user_id",
			event:  map[string]any{"user_id": nil, "is_test": false},
			total:  100.0,
			reason: "drop_if: user_id is null",
		},
		{
			name:   "test_event",
			event:  map[string]any{"user_id": "usr_123", "is_test": true},
			total:  100.0,
			reason: "drop_if: is_test is true",
		},
		{
			name:   "zero_total",
			event:  map[string]any{"user_id": "usr_123", "is_test": false},
			total:  0.0,
			reason: "keep_if: order_total is 0",
		},
		{
			name:   "valid_event",
			event:  map[string]any{"user_id": "usr_123", "is_test": false},
			total:  50.0,
			reason: "PASS",
		},
	}

	for _, tc := range testCases {
		activation := map[string]any{
			"event":       tc.event,
			"order_total": tc.total,
		}

		dropResult, _, _ := dropIfProg.Eval(activation)
		keepResult, _, _ := keepIfProg.Eval(activation)

		dropped := dropResult.Value().(bool) || !keepResult.Value().(bool)
		status := "KEEP"
		if dropped {
			status = "DROP"
		}

		fmt.Printf("%s: %s (%s)\n", tc.name, status, tc.reason)
	}

	// Output:
	// null_user_id: DROP (drop_if: user_id is null)
	// test_event: DROP (drop_if: is_test is true)
	// zero_total: DROP (keep_if: order_total is 0)
	// valid_event: KEEP (PASS)
}

// Example_orderTotalCalculation shows the order total aggregation in detail.
func Example_orderTotalCalculation() {
	env, _ := cel.NewEnv(
		append(ext.AllFuncs(),
			cel.Variable("items", cel.ListType(cel.MapType(cel.StringType, cel.DynType))),
		)...,
	)

	// expr: sumDouble(items.map(i, i.price * toDouble(i.quantity)))
	ast, _ := env.Compile(`sumDouble(items.map(i, i.price * toDouble(i.quantity)))`)
	prog, _ := env.Program(ast)

	items := []any{
		map[string]any{"sku": "WIDGET-A", "price": 29.99, "quantity": int64(2)},
		map[string]any{"sku": "WIDGET-B", "price": 49.99, "quantity": int64(1)},
		map[string]any{"sku": "WIDGET-C", "price": 19.99, "quantity": int64(3)},
	}

	result, _, _ := prog.Eval(map[string]any{"items": items})

	fmt.Println("Items:")
	for _, item := range items {
		m := item.(map[string]any)
		lineTotal := m["price"].(float64) * float64(m["quantity"].(int64))
		fmt.Printf("  %s: %.2f x %d = %.2f\n", m["sku"], m["price"], m["quantity"], lineTotal)
	}
	fmt.Printf("Order Total: %.2f\n", result.Value())

	// Output:
	// Items:
	//   WIDGET-A: 29.99 x 2 = 59.98
	//   WIDGET-B: 49.99 x 1 = 49.99
	//   WIDGET-C: 19.99 x 3 = 59.97
	// Order Total: 169.94
}

// Example_nullHandling shows coalesce and ifNull behavior.
func Example_nullHandling() {
	env, _ := cel.NewEnv(
		append(ext.AllFuncs(),
			cel.Variable("event", cel.MapType(cel.StringType, cel.DynType)),
		)...,
	)

	expressions := []struct {
		name string
		expr string
	}{
		{"currency_default", `orDefault(event.currency, 'USD')`},
		{"discount_default", `ifNull(event.discount_percent, 0.0)`},
		{"coalesce_email", `coalesce(event.work_email, event.personal_email)`},
		{"is_currency_null", `isNull(event.currency)`},
	}

	event := map[string]any{
		"currency":         nil,
		"discount_percent": nil,
		"work_email":       nil,
		"personal_email":   "user@personal.com",
	}

	fmt.Println("Event: currency=null, discount_percent=null, work_email=null, personal_email=user@personal.com")
	fmt.Println()

	for _, e := range expressions {
		ast, _ := env.Compile(e.expr)
		prog, _ := env.Program(ast)
		result, _, _ := prog.Eval(map[string]any{"event": event})
		fmt.Printf("%s: %v\n", e.name, result.Value())
	}

	// Output:
	// Event: currency=null, discount_percent=null, work_email=null, personal_email=user@personal.com
	//
	// currency_default: USD
	// discount_default: 0
	// coalesce_email: user@personal.com
	// is_currency_null: true
}

// Example_hashComparison shows xxhash vs sha256 output.
func Example_hashComparison() {
	env, _ := cel.NewEnv(
		append(ext.AllFuncs(),
			cel.Variable("email", cel.StringType),
		)...,
	)

	email := "john.doe@example.com"

	xxhashAST, _ := env.Compile(`xxhash(email)`)
	xxhashProg, _ := env.Program(xxhashAST)
	xxhashResult, _, _ := xxhashProg.Eval(map[string]any{"email": email})

	sha256AST, _ := env.Compile(`sha256(email)`)
	sha256Prog, _ := env.Program(sha256AST)
	sha256Result, _, _ := sha256Prog.Eval(map[string]any{"email": email})

	fmt.Printf("Email: %s\n", email)
	fmt.Printf("xxhash:  %s (16 chars)\n", xxhashResult.Value())
	fmt.Printf("sha256:  %s (64 chars)\n", sha256Result.Value())

	// Output:
	// Email: john.doe@example.com
	// xxhash:  7ea731dc1cc36e63 (16 chars)
	// sha256:  836f82db99121b3481011f16b49dfa5fbc714a0d1b1b9f784a1ebbbf5b39577f (64 chars)
}

// Example_temporalPartitioning shows date-based partitioning.
func Example_temporalPartitioning() {
	env, _ := cel.NewEnv(
		append(ext.AllFuncs(),
			cel.Variable("_occurred_at", cel.TimestampType),
		)...,
	)

	occurredAt := time.Date(2024, 6, 15, 14, 30, 45, 0, time.UTC)

	expressions := []struct {
		name string
		expr string
	}{
		{"date", `date(_occurred_at)`},
		{"year", `year(_occurred_at)`},
		{"month", `month(_occurred_at)`},
		{"day", `day(_occurred_at)`},
		{"hour", `hour(_occurred_at)`},
		{"dayOfWeek", `dayOfWeek(_occurred_at)`},
		{"weekOfYear", `weekOfYear(_occurred_at)`},
		{"partition_key", `date(_occurred_at) + '/hour=' + padLeft(toString(hour(_occurred_at)), 2, '0')`},
	}

	fmt.Printf("Timestamp: %s\n\n", occurredAt.Format(time.RFC3339))

	for _, e := range expressions {
		ast, _ := env.Compile(e.expr)
		prog, _ := env.Program(ast)
		result, _, _ := prog.Eval(map[string]any{"_occurred_at": occurredAt})
		fmt.Printf("%s: %v\n", e.name, result.Value())
	}

	// Output:
	// Timestamp: 2024-06-15T14:30:45Z
	//
	// date: 2024-06-15
	// year: 2024
	// month: 6
	// day: 15
	// hour: 14
	// dayOfWeek: 6
	// weekOfYear: 24
	// partition_key: 2024-06-15/hour=14
}

// Example_listAggregations shows list functions.
func Example_listAggregations() {
	env, _ := cel.NewEnv(
		append(ext.AllFuncs(),
			cel.Variable("prices", cel.ListType(cel.DoubleType)),
			cel.Variable("quantities", cel.ListType(cel.IntType)),
		)...,
	)

	prices := []float64{29.99, 49.99, 19.99, 99.99}
	quantities := []int64{2, 1, 3, 1}

	expressions := []struct {
		name string
		expr string
	}{
		{"sumDouble", `sumDouble(prices)`},
		{"minDouble", `minDouble(prices)`},
		{"maxDouble", `maxDouble(prices)`},
		{"avgDouble", `avgDouble(prices)`},
		{"sum", `sum(quantities)`},
		{"first", `first(prices)`},
		{"last", `last(prices)`},
	}

	fmt.Printf("Prices: %v\n", prices)
	fmt.Printf("Quantities: %v\n\n", quantities)

	for _, e := range expressions {
		ast, _ := env.Compile(e.expr)
		prog, _ := env.Program(ast)
		result, _, _ := prog.Eval(map[string]any{
			"prices":     prices,
			"quantities": quantities,
		})
		fmt.Printf("%s: %v\n", e.name, result.Value())
	}

	// Output:
	// Prices: [29.99 49.99 19.99 99.99]
	// Quantities: [2 1 3 1]
	//
	// sumDouble: 199.95999999999998
	// minDouble: 19.99
	// maxDouble: 99.99
	// avgDouble: 49.989999999999995
	// sum: 7
	// first: 29.99
	// last: 99.99
}
