package metrics

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"strings"
	"time"

	"github.com/containers/kubernetes-mcp-server/pkg/api"
	ammodels "github.com/prometheus/alertmanager/api/v2/models"
	"github.com/prometheus/common/model"
	"k8s.io/utils/ptr"

	"github.com/rhobs/obs-mcp/pkg/metrics/alertmanager"
	"github.com/rhobs/obs-mcp/pkg/metrics/prometheus"
)

const (
	// millisecondsPerSecond converts Prometheus millisecond timestamps to seconds.
	millisecondsPerSecond = 1000
)

// getString is a helper to extract a string parameter with a default value.
func getString(params map[string]any, key, defaultValue string) string {
	if val, ok := params[key]; ok {
		if str, ok := val.(string); ok && str != "" {
			return str
		}
	}
	return defaultValue
}

// getBoolPtr is a helper to extract an optional boolean parameter as a pointer.
func getBoolPtr(params map[string]any, key string) *bool {
	if val, ok := params[key]; ok {
		if b, ok := val.(bool); ok {
			return &b
		}
	}
	return nil
}

// parseDefaultTimeRange parses optional start/end time strings,
// defaulting to the last hour if both are empty.
func parseDefaultTimeRange(start, end string) (startTime, endTime time.Time, err error) {
	if start == "" && end == "" {
		endTime = time.Now()
		startTime = endTime.Add(-prometheus.ListMetricsTimeRange)
		return startTime, endTime, nil
	}

	if start != "" {
		startTime, err = prometheus.ParseTimestamp(start)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid start time format: %w", err)
		}
	}
	if end != "" {
		endTime, err = prometheus.ParseTimestamp(end)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid end time format: %w", err)
		}
	}
	return startTime, endTime, nil
}

// parseFilterString splits a comma-separated filter string into trimmed parts.
func parseFilterString(filter string) []string {
	if filter == "" {
		return nil
	}
	parts := strings.Split(filter, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

// convertAlert converts an Alertmanager GettableAlert to the Alert output type.
func convertAlert(a *ammodels.GettableAlert) Alert {
	labels := make(map[string]string)
	maps.Copy(labels, a.Labels)

	annotations := make(map[string]string)
	maps.Copy(annotations, a.Annotations)

	var silencedBy, inhibitedBy []string
	var state string
	if a.Status != nil {
		if a.Status.SilencedBy != nil {
			silencedBy = a.Status.SilencedBy
		}
		if a.Status.InhibitedBy != nil {
			inhibitedBy = a.Status.InhibitedBy
		}
		state = ptr.Deref(a.Status.State, "")
	}
	if silencedBy == nil {
		silencedBy = []string{}
	}
	if inhibitedBy == nil {
		inhibitedBy = []string{}
	}

	var startsAt, endsAt string
	if a.StartsAt != nil {
		startsAt = a.StartsAt.String()
	}
	if a.EndsAt != nil {
		endsAt = a.EndsAt.String()
	}

	return Alert{
		Labels:      labels,
		Annotations: annotations,
		StartsAt:    startsAt,
		EndsAt:      endsAt,
		Status: AlertStatus{
			State:       state,
			SilencedBy:  silencedBy,
			InhibitedBy: inhibitedBy,
		},
	}
}

// convertMatcher converts an Alertmanager Matcher to the Matcher output type.
func convertMatcher(m *ammodels.Matcher) Matcher {
	isEqual := true
	if m.IsEqual != nil {
		isEqual = *m.IsEqual
	}
	return Matcher{
		Name:    ptr.Deref(m.Name, ""),
		Value:   ptr.Deref(m.Value, ""),
		IsRegex: m.IsRegex != nil && *m.IsRegex,
		IsEqual: isEqual,
	}
}

// convertSilence converts an Alertmanager GettableSilence to the Silence output type.
func convertSilence(s *ammodels.GettableSilence) Silence {
	matchers := make([]Matcher, len(s.Matchers))
	for i, m := range s.Matchers {
		matchers[i] = convertMatcher(m)
	}

	var state string
	if s.Status != nil {
		state = ptr.Deref(s.Status.State, "")
	}

	var startsAt, endsAt string
	if s.StartsAt != nil {
		startsAt = s.StartsAt.String()
	}
	if s.EndsAt != nil {
		endsAt = s.EndsAt.String()
	}

	return Silence{
		ID: ptr.Deref(s.ID, ""),
		Status: SilenceStatus{
			State: state,
		},
		Matchers:  matchers,
		StartsAt:  startsAt,
		EndsAt:    endsAt,
		CreatedBy: ptr.Deref(s.CreatedBy, ""),
		Comment:   ptr.Deref(s.Comment, ""),
	}
}

// listMetrics handles the listing of available Prometheus metrics.
func listMetrics(ctx context.Context, promClient prometheus.Loader, args map[string]any) (*api.ToolCallResult, error) {
	// Validate required parameters
	nameRegex := getString(args, "name_regex", "")
	if nameRegex == "" {
		return api.NewToolCallResult("", fmt.Errorf("name_regex parameter is required and must be a string")), nil
	}

	metrics, err := promClient.ListMetrics(ctx, nameRegex)
	if err != nil {
		slog.Error("failed to list metrics", "error", err)
		return api.NewToolCallResult("", fmt.Errorf("failed to list metrics: %w", err)), nil
	}

	slog.Info("listMetricsHandler executed successfully", "resultLength", len(metrics))
	return api.NewToolCallResultStructured(ListMetricsOutput{Metrics: metrics}, nil), nil
}

func listMetricsHandler(params api.ToolHandlerParams) (*api.ToolCallResult, error) {
	slog.Info("listMetricsHandler called")

	promClient, err := getPromClient(params)
	if err != nil {
		return api.NewToolCallResult("", fmt.Errorf("failed to create Prometheus client: %w", err)), nil
	}

	return listMetrics(params.Context, promClient, params.GetArguments())
}

// executeRangeQuery handles the execution of Prometheus range queries.
func executeRangeQuery(ctx context.Context, promClient prometheus.Loader, args map[string]any, fullResponse bool) (*api.ToolCallResult, error) {
	// Validate required parameters
	query := getString(args, "query", "")
	step := getString(args, "step", "")
	start := getString(args, "start", "")
	end := getString(args, "end", "")
	duration := getString(args, "duration", "")

	if query == "" {
		return api.NewToolCallResult("", fmt.Errorf("query parameter is required and must be a string")), nil
	}
	if step == "" {
		return api.NewToolCallResult("", fmt.Errorf("step parameter is required and must be a string")), nil
	}

	// Parse step duration
	stepDuration, err := model.ParseDuration(step)
	if err != nil {
		return api.NewToolCallResult("", fmt.Errorf("invalid step format: %w", err)), nil
	}

	if (start == "") != (end == "") {
		return api.NewToolCallResult("", fmt.Errorf("both start and end must be provided together")), nil
	}

	var startTime, endTime time.Time

	if start != "" && end != "" {
		// Handle explicit start/end times
		startTime, err = prometheus.ParseTimestamp(start)
		if err != nil {
			return api.NewToolCallResult("", fmt.Errorf("invalid start time format: %w", err)), nil
		}
		endTime, err = prometheus.ParseTimestamp(end)
		if err != nil {
			return api.NewToolCallResult("", fmt.Errorf("invalid end time format: %w", err)), nil
		}
	} else {
		// Handle duration-based query (default to 1h if nothing specified)
		durationStr := duration
		if durationStr == "" {
			durationStr = "1h"
		}
		dur, err := model.ParseDuration(durationStr)
		if err != nil {
			return api.NewToolCallResult("", fmt.Errorf("invalid duration format: %w", err)), nil
		}
		endTime = time.Now()
		startTime = endTime.Add(-time.Duration(dur))
	}

	// Execute the range query
	result, err := promClient.ExecuteRangeQuery(ctx, query, startTime, endTime, time.Duration(stepDuration))
	if err != nil {
		return api.NewToolCallResult("", fmt.Errorf("failed to execute range query: %w", err)), nil
	}

	// Convert to structured output
	output := RangeQueryOutput{
		ResultType: fmt.Sprintf("%v", result["resultType"]),
	}

	resMatrix, ok := result["result"].(model.Matrix)
	if ok {
		slog.Info("executeRangeQuery executed successfully", "resultLength", resMatrix.Len())

		if fullResponse {
			// Return full data
			output.Result = make([]SeriesResult, len(resMatrix))
			for i, series := range resMatrix {
				labels := make(map[string]string)
				for k, v := range series.Metric {
					labels[string(k)] = string(v)
				}
				values := make([][]any, len(series.Values))
				for j, sample := range series.Values {
					values[j] = []any{float64(sample.Timestamp) / millisecondsPerSecond, sample.Value.String()}
				}
				output.Result[i] = SeriesResult{
					Metric: labels,
					Values: values,
				}
			}
		} else {
			// Return summary statistics instead of full data
			output.Summary = make([]SeriesResultSummary, len(resMatrix))
			for i, series := range resMatrix {
				output.Summary[i] = CalculateSeriesSummary(series.Metric, series.Values)
			}
		}
	} else {
		slog.Info("executeRangeQuery executed successfully (unknown format)", "result", result)
	}

	if warnings, ok := result["warnings"].([]string); ok {
		output.Warnings = warnings
	}

	return api.NewToolCallResultStructured(output, nil), nil
}

func executeRangeQueryHandler(params api.ToolHandlerParams) (*api.ToolCallResult, error) {
	slog.Info("executeRangeQueryHandler called")

	promClient, err := getPromClient(params)
	if err != nil {
		return api.NewToolCallResult("", fmt.Errorf("failed to create Prometheus client: %w", err)), nil
	}

	cfg := getConfig(params)
	return executeRangeQuery(params.Context, promClient, params.GetArguments(), cfg.RangeQueryFullResponse)
}

// showTimeseriesHandler handles the show_timeseries tool, validating the query for UI chart rendering.
func showTimeseriesHandler(params api.ToolHandlerParams) (*api.ToolCallResult, error) {
	slog.Info("showTimeseriesHandler called")

	promClient, err := getPromClient(params)
	if err != nil {
		return api.NewToolCallResult("", fmt.Errorf("failed to create Prometheus client: %w", err)), nil
	}

	// Executing the query handler just to validate the query is correct.
	result, err := executeRangeQuery(params.Context, promClient, params.GetArguments(), true)
	if err != nil {
		return result, err
	}
	if result.Error != nil {
		return result, nil //nolint:nilerr // result carries the MCP-level error; the Go error is intentionally nil
	}

	// For UI purposes only, no additional data to be sent to the LLM context.
	return api.NewToolCallResultStructured(struct{}{}, nil), nil
}

// executeInstantQuery handles the execution of Prometheus instant queries.
func executeInstantQuery(ctx context.Context, promClient prometheus.Loader, args map[string]any) (*api.ToolCallResult, error) {
	// Validate required parameters
	query := getString(args, "query", "")
	if query == "" {
		return api.NewToolCallResult("", fmt.Errorf("query parameter is required and must be a string")), nil
	}

	timeStr := getString(args, "time", "")
	var queryTime time.Time
	var err error
	if timeStr == "" {
		queryTime = time.Now()
	} else {
		queryTime, err = prometheus.ParseTimestamp(timeStr)
		if err != nil {
			return api.NewToolCallResult("", fmt.Errorf("invalid time format: %w", err)), nil
		}
	}

	// Execute the instant query
	result, err := promClient.ExecuteInstantQuery(ctx, query, queryTime)
	if err != nil {
		return api.NewToolCallResult("", fmt.Errorf("failed to execute instant query: %w", err)), nil
	}

	// Convert to structured output
	output := InstantQueryOutput{
		ResultType: fmt.Sprintf("%v", result["resultType"]),
	}

	resVector, ok := result["result"].(model.Vector)
	if ok {
		slog.Info("executeInstantQueryHandler executed successfully", "resultLength", len(resVector))

		output.Result = make([]InstantResult, len(resVector))
		for i, sample := range resVector {
			labels := make(map[string]string)
			for k, v := range sample.Metric {
				labels[string(k)] = string(v)
			}
			output.Result[i] = InstantResult{
				Metric: labels,
				Value:  []any{float64(sample.Timestamp) / millisecondsPerSecond, sample.Value.String()},
			}
		}
	} else {
		slog.Info("executeInstantQueryHandler executed successfully (unknown format)", "result", result)
	}

	if warnings, ok := result["warnings"].([]string); ok {
		output.Warnings = warnings
	}

	return api.NewToolCallResultStructured(output, nil), nil
}

func executeInstantQueryHandler(params api.ToolHandlerParams) (*api.ToolCallResult, error) {
	slog.Info("executeInstantQueryHandler called")

	promClient, err := getPromClient(params)
	if err != nil {
		return api.NewToolCallResult("", fmt.Errorf("failed to create Prometheus client: %w", err)), nil
	}

	return executeInstantQuery(params.Context, promClient, params.GetArguments())
}

// getLabelNames handles the retrieval of label names.
func getLabelNames(ctx context.Context, promClient prometheus.Loader, args map[string]any) (*api.ToolCallResult, error) {
	metric := getString(args, "metric", "")
	start := getString(args, "start", "")
	end := getString(args, "end", "")

	startTime, endTime, err := parseDefaultTimeRange(start, end)
	if err != nil {
		return api.NewToolCallResult("", err), nil
	}

	// Get label names
	labels, err := promClient.GetLabelNames(ctx, metric, startTime, endTime)
	if err != nil {
		return api.NewToolCallResult("", fmt.Errorf("failed to get label names: %w", err)), nil
	}

	slog.Info("getLabelNamesHandler executed successfully", "labelCount", len(labels))
	return api.NewToolCallResultStructured(LabelNamesOutput{Labels: labels}, nil), nil
}

func getLabelNamesHandler(params api.ToolHandlerParams) (*api.ToolCallResult, error) {
	slog.Info("getLabelNamesHandler called")

	promClient, err := getPromClient(params)
	if err != nil {
		return api.NewToolCallResult("", fmt.Errorf("failed to create Prometheus client: %w", err)), nil
	}

	return getLabelNames(params.Context, promClient, params.GetArguments())
}

// getLabelValues handles the retrieval of label values.
func getLabelValues(ctx context.Context, promClient prometheus.Loader, args map[string]any) (*api.ToolCallResult, error) {
	// Validate required parameters
	label := getString(args, "label", "")
	if label == "" {
		return api.NewToolCallResult("", fmt.Errorf("label parameter is required and must be a string")), nil
	}

	metric := getString(args, "metric", "")
	start := getString(args, "start", "")
	end := getString(args, "end", "")

	startTime, endTime, err := parseDefaultTimeRange(start, end)
	if err != nil {
		return api.NewToolCallResult("", err), nil
	}

	// Get label values
	values, err := promClient.GetLabelValues(ctx, label, metric, startTime, endTime)
	if err != nil {
		return api.NewToolCallResult("", fmt.Errorf("failed to get label values: %w", err)), nil
	}

	slog.Info("getLabelValuesHandler executed successfully", "valueCount", len(values))
	return api.NewToolCallResultStructured(LabelValuesOutput{Values: values}, nil), nil
}

func getLabelValuesHandler(params api.ToolHandlerParams) (*api.ToolCallResult, error) {
	slog.Info("getLabelValuesHandler called")

	promClient, err := getPromClient(params)
	if err != nil {
		return api.NewToolCallResult("", fmt.Errorf("failed to create Prometheus client: %w", err)), nil
	}

	return getLabelValues(params.Context, promClient, params.GetArguments())
}

// getSeries handles the retrieval of time series.
func getSeries(ctx context.Context, promClient prometheus.Loader, args map[string]any) (*api.ToolCallResult, error) {
	// Validate required parameters
	matchesStr := getString(args, "matches", "")
	if matchesStr == "" {
		return api.NewToolCallResult("", fmt.Errorf("matches parameter is required and must be a string")), nil
	}

	matches := []string{matchesStr}
	start := getString(args, "start", "")
	end := getString(args, "end", "")

	startTime, endTime, err := parseDefaultTimeRange(start, end)
	if err != nil {
		return api.NewToolCallResult("", err), nil
	}

	// Get series
	series, err := promClient.GetSeries(ctx, matches, startTime, endTime)
	if err != nil {
		return api.NewToolCallResult("", fmt.Errorf("failed to get series: %w", err)), nil
	}

	slog.Info("getSeriesHandler executed successfully", "cardinality", len(series))
	return api.NewToolCallResultStructured(SeriesOutput{
		Series:      series,
		Cardinality: len(series),
	}, nil), nil
}

func getSeriesHandler(params api.ToolHandlerParams) (*api.ToolCallResult, error) {
	slog.Info("getSeriesHandler called")

	promClient, err := getPromClient(params)
	if err != nil {
		return api.NewToolCallResult("", fmt.Errorf("failed to create Prometheus client: %w", err)), nil
	}

	return getSeries(params.Context, promClient, params.GetArguments())
}

// getAlerts handles the retrieval of alerts from Alertmanager.
func getAlerts(ctx context.Context, amClient alertmanager.Loader, args map[string]any) (*api.ToolCallResult, error) {
	alerts, err := amClient.GetAlerts(
		ctx,
		getBoolPtr(args, "active"),
		getBoolPtr(args, "silenced"),
		getBoolPtr(args, "inhibited"),
		getBoolPtr(args, "unprocessed"),
		parseFilterString(getString(args, "filter", "")),
		getString(args, "receiver", ""),
	)
	if err != nil {
		return api.NewToolCallResult("", fmt.Errorf("failed to get alerts: %w", err)), nil
	}

	output := AlertsOutput{
		Alerts: make([]Alert, len(alerts)),
	}
	for i, alert := range alerts {
		output.Alerts[i] = convertAlert(alert)
	}

	slog.Info("getAlertsHandler executed successfully", "alertCount", len(alerts))
	return api.NewToolCallResultStructured(output, nil), nil
}

func getAlertsHandler(params api.ToolHandlerParams) (*api.ToolCallResult, error) {
	slog.Info("getAlertsHandler called")

	amClient, err := getAlertmanagerClient(params)
	if err != nil {
		return api.NewToolCallResult("", fmt.Errorf("failed to create Alertmanager client: %w", err)), nil
	}

	return getAlerts(params.Context, amClient, params.GetArguments())
}

// getSilences handles the retrieval of silences from Alertmanager.
func getSilences(ctx context.Context, amClient alertmanager.Loader, args map[string]any) (*api.ToolCallResult, error) {
	silences, err := amClient.GetSilences(ctx, parseFilterString(getString(args, "filter", "")))
	if err != nil {
		return api.NewToolCallResult("", fmt.Errorf("failed to get silences: %w", err)), nil
	}

	output := SilencesOutput{
		Silences: make([]Silence, len(silences)),
	}
	for i, silence := range silences {
		output.Silences[i] = convertSilence(silence)
	}

	slog.Info("getSilencesHandler executed successfully", "silenceCount", len(silences))
	return api.NewToolCallResultStructured(output, nil), nil
}

func getSilencesHandler(params api.ToolHandlerParams) (*api.ToolCallResult, error) {
	slog.Info("getSilencesHandler called")

	amClient, err := getAlertmanagerClient(params)
	if err != nil {
		return api.NewToolCallResult("", fmt.Errorf("failed to create Alertmanager client: %w", err)), nil
	}

	return getSilences(params.Context, amClient, params.GetArguments())
}
