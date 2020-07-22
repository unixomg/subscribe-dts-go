package alisdk

import (
	"encoding/json"
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk"
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/requests"
	"goimpl/common"
	//"fmt"
)

func CreateConsumerGroup(subscriptionInstanceId,consumerGroupName,consumerGroupUserName,consumerGroupPassword string) (err error,dtsres *common.DtsResp) {
	dtsres = new(common.DtsResp)
	client, err := sdk.NewClientWithAccessKey("cn-beijing", common.DtsAccesKey, common.DtsSecretKey)
	if err != nil {
		return err,dtsres
	}
	request := requests.NewCommonRequest()
	request.Method = "POST"
	request.Scheme = "https" // https | http
	request.Domain = "dts.aliyuncs.com"
	request.Version = "2020-01-01"
	request.ApiName = "CreateConsumerGroup"
	request.QueryParams["RegionId"] = "cn-beijing"
	request.QueryParams["SubscriptionInstanceId"] = subscriptionInstanceId
	request.QueryParams["ConsumerGroupName"] = consumerGroupName
	request.QueryParams["ConsumerGroupUserName"] = consumerGroupUserName
	request.QueryParams["ConsumerGroupPassword"] = consumerGroupPassword
	response, err := client.ProcessCommonRequest(request)
	if err != nil {
		return err,dtsres
	}
	result := response.GetHttpContentString()
	if err = json.Unmarshal([]byte(result),dtsres);err != nil {
		return err,dtsres
	}
	return nil,dtsres
}

func DescribeConsumerGroup(subscriptionInstanceId string) (err error,dtsdescres *common.DtsDescResp) {
	dtsdescres = new(common.DtsDescResp)
	client, err := sdk.NewClientWithAccessKey("cn-beijing", common.DtsAccesKey, common.DtsSecretKey)
	if err != nil {
		return err,dtsdescres
	}

	request := requests.NewCommonRequest()
	request.Method = "POST"
	request.Scheme = "https" // https | http
	request.Domain = "dts.aliyuncs.com"
	request.Version = "2020-01-01"
	request.ApiName = "DescribeConsumerGroup"
	request.QueryParams["RegionId"] = "cn-beijing"
	request.QueryParams["SubscriptionInstanceId"] = subscriptionInstanceId

	response, err := client.ProcessCommonRequest(request)
	if err != nil {
		return err,dtsdescres
	}
	//fmt.Println(response.GetHttpContentString())
	err = json.Unmarshal(response.GetHttpContentBytes(),&dtsdescres)
	if err != nil {
		return err,dtsdescres
	}
	//fmt.Printf("%+v\n",dtsdescres)
	return nil,dtsdescres
}

func DescribeSubscriptionInstanceStatus(subscriptionInstanceId string) (err error,dtsdescinsstatus *common.DtsDescInstanceStatus)  {
	dtsdescinsstatus = new(common.DtsDescInstanceStatus)
	client, err := sdk.NewClientWithAccessKey("cn-beijing", common.DtsAccesKey, common.DtsSecretKey)
	if err != nil {
		return err,dtsdescinsstatus
	}
	request := requests.NewCommonRequest()
	request.Method = "POST"
	request.Scheme = "https" // https | http
	request.Domain = "dts.aliyuncs.com"
	request.Version = "2020-01-01"
	request.ApiName = "DescribeSubscriptionInstanceStatus"
	request.QueryParams["RegionId"] = "cn-beijing"
	request.QueryParams["SubscriptionInstanceId"] = subscriptionInstanceId
	response ,err :=client.ProcessCommonRequest(request)
	if err != nil {
		return err,dtsdescinsstatus
	}
	//fmt.Println(response)
	err = json.Unmarshal(response.GetHttpContentBytes(),&dtsdescinsstatus)
	if err != nil {
		return err,dtsdescinsstatus
	}
	//fmt.Printf("%+v\n",dtsdescinsstatus)
	return nil,dtsdescinsstatus
}

func buildSubscriptionObjects(subDBName string,subTables []string) common.SubscriptionObjects {
	ModifyTableObj := common.ModifyTableObj{}

	ModifyTableObjList := make([]common.ModifyTableObj,0)
	for _,table := range subTables{
		ModifyTableObj.TableName = table
		ModifyTableObj.NewTableName = table
		ModifyTableObjList = append(ModifyTableObjList,ModifyTableObj)
	}

	ModifyDtsObj := common.ModifyDtsObj{DBName: subDBName,TableIncludes: ModifyTableObjList}
	ModifyDtsObjList := make([]common.ModifyDtsObj,0)
	ModifyDtsObjList = append(ModifyDtsObjList,ModifyDtsObj)
	SubscriptionObjects := common.SubscriptionObjects{
		SubscriptionObjects: ModifyDtsObjList,
	}
	return SubscriptionObjects
}


func ModifySubscriptionObject(subscriptionInstanceId string,subDBName string,subtables []string) (err error,modifyResp *common.ModifyResp) {
	modifyResp  = &common.ModifyResp{}
	subscriptionObjects := buildSubscriptionObjects(subDBName,subtables)
	//fmt.Printf("%+v\n",subscriptionObjects)
	subbyte,err := json.Marshal(subscriptionObjects.SubscriptionObjects)
	if err != nil {
		return err,modifyResp
	}
	//fmt.Println(string(subbyte))
	//fmt.Println(strconv.Quote(string(subbyte)))

	//aa:= "[{\n" + "\"DBName\":\"olap\","+ "\"TableIncludes\":[" + "{\"TableName\":\"dtstest\"}," + "{\"TableName\":\"dtstest1\"}," + "]"+ "}]"
	//fmt.Println(aa)
	client, err := sdk.NewClientWithAccessKey("cn-beijing", common.DtsAccesKey, common.DtsSecretKey)
	if err != nil {
		return err,modifyResp
	}
	request := requests.NewCommonRequest()
	request.Method = "POST"
	request.Scheme = "https" // https | http
	request.Domain = "dts.aliyuncs.com"
	request.Version = "2020-01-01"
	request.ApiName = "ModifySubscriptionObject"
	request.QueryParams["RegionId"] = "cn-beijing"
	request.QueryParams["SubscriptionInstanceId"] = subscriptionInstanceId
	request.QueryParams["SubscriptionObject"] = string(subbyte)
	response, err := client.ProcessCommonRequest(request)
	if err != nil {
		return err,modifyResp
	}
	//fmt.Print(response.GetHttpContentString())
	err = json.Unmarshal(response.GetHttpContentBytes(),modifyResp)
	if err != nil {
		return err,modifyResp
	}
	return nil,modifyResp
}














