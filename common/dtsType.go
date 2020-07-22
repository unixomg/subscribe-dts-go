package common

type DtsResp struct {
	ConsumerGroupID string `json:ConsumerGroupID`
	RequestId  string `json:RquestId`
	Success bool `json:Succecss`
}

type ConsummerItem struct {
	ConsumptionCheckpoint string
	ConsumerGroupID string
	ConsumerGroupUserName string
	ConsumerGroupName string
	MessageDelay int
	UnconsumedData int
}

type DescribeConsumerChannel struct {
	DescribeConsumerChannel []ConsummerItem
}

type DtsDescResp struct {
	TotalRecordCount int
	PageRecordCount int
	RequestId string
	PageNumber int
	ConsumerChannels DescribeConsumerChannel
}

type SubscriptionHost struct {
	PublicHost string
	PrivateHost string
	VPCHost string
}
type Tables struct {
	Table []string
}

type SynchronousObjectItem struct {
	DatabaseName string
	WholeDatabase bool
	TableList Tables

}
type SubscriptionObject struct {
	SynchronousObject []SynchronousObjectItem
}

type SubscriptionDataType struct {
	DML bool
	DDL bool
}

type SourceEndpoint struct {
	InstanceID string
	InstanceType string
}

type DtsDescInstanceStatus struct {
	Status string
	RequestId string
	SubscriptionHost SubscriptionHost
	EndTimestamp string
	PayType string
	SubscriptionInstanceID string
	SubscriptionObject SubscriptionObject
	SubscriptionDataType SubscriptionDataType
	SubscriptionInstanceName string
	SubscribeTopic string
	SourceEndpoint SourceEndpoint
	BeginTimestamp string
}

type ModifyTableObj struct {
	TableName string
	NewTableName string
}

type ModifyDtsObj struct {
	DBName string
	TableIncludes []ModifyTableObj
}

type SubscriptionObjects struct {
	SubscriptionObjects []ModifyDtsObj
}

type ModifyResp struct {
	RequestId string
	Success bool
}