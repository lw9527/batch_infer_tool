package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
)
// 获取引擎监控路数，如果总路数为0，服务未启动
func getServiceInfo(serviceId string) (int, int, int) {

    vmAddr := "http://vmselect.huabei.xf-yun.com/select/2/prometheus/api/v1/query"

    entTotalPql := fmt.Sprintf("max by (dc, sub, ent) (engine_ent_total{dc='dx', ent=~'%s', sub='olm'})", serviceId)
    entUsedPql := fmt.Sprintf("max by (dc, sub, ent) (engine_ent_use{dc='dx', ent=~'%s', sub='olm'})", serviceId)
    entNodeTotalPql := fmt.Sprintf("max by (dc, sub, ent) (engine_ent_node_total{dc='dx', ent=~'%s', sub='olm'})", serviceId)

    PromDeployValue := QueryFromPrometheus(vmAddr, entTotalPql)
    PromUsedValue := QueryFromPrometheus(vmAddr, entUsedPql)
    PromNodeTotalValue := QueryFromPrometheus(vmAddr, entNodeTotalPql)
	fmt.Sprintf("%v %v %v ",PromDeployValue ,PromUsedValue, PromNodeTotalValue)
    return int(PromDeployValue), int(PromUsedValue), int(PromNodeTotalValue)
}
func QueryFromPrometheus(host, query string) float64 {
	var result float64

	req, _ := http.NewRequest("GET", host, nil)

	q := req.URL.Query()
	q.Add("query", query)
	req.URL.RawQuery = q.Encode()

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		logInfo("query from prometheus error: %v,url: %v \n", err, req.URL.String())
		return result
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		logInfo("read response body error: %v", err)
		return result
	}

	var promResp PrometheusResponse
	if err = json.Unmarshal(body, &promResp); err != nil {
		logInfo("unmarshal response body error: %v", err)
		return result
	}

	if len(promResp.Data.Result) > 0 {
		f, err := strconv.ParseFloat(promResp.Data.Result[0].Value[1].(string), 64)
		if err != nil {
			logInfo("parse string %v to float64 error: %v", promResp.Data.Result[0].Value[1], err)
		} else {
			result = f
		}
	}

	return result
}

type PrometheusResponse struct {
    Status string            `json:"status"`
    Data   ServiceidResponse `json:"data"`
}

type ServiceidResponse struct {
    ResultType string   `json:"resultType"`
    Result     []Result `json:"result"`
}

type Result struct {
    //Metric Metric        `json:"metric"`
    Value []interface{} `json:"value"`
}