package config

import (
	"github.com/nacos-group/nacos-sdk-go/clients"
	"github.com/nacos-group/nacos-sdk-go/clients/config_client"
	"goimpl/common"

	"github.com/nacos-group/nacos-sdk-go/common/constant"
	"github.com/nacos-group/nacos-sdk-go/vo"
	"github.com/spf13/viper"
	"strings"
)

type AcmItem struct {
	Id string
	Group string
}


var v *viper.Viper
var configClient config_client.IConfigClient
var err error

func GetAcmConfig() *viper.Viper {
	return v
}

func GetConfigClient() config_client.IConfigClient {
	return configClient
}


func InitAcmItem(env string,keys ...AcmItem) (err error) {
	var (
		namespaceId,endPoint string
	)
	switch env {
	case "test":
		namespaceId = ""
		endPoint =""
	case "prod":
		namespaceId = ""
		endPoint = ""
	case "","dev":
		namespaceId = common.DevAcmNameSpaceId
		endPoint = common.DevAcmEndPoint
	default:
		panic("invald env")
	}

	clientConfig := constant.ClientConfig{
		Endpoint: endPoint,
		NamespaceId: namespaceId,
		AccessKey: common.AccessKey,
		SecretKey: common.SecretKey,
		TimeoutMs: 5*1000,
		ListenInterval: 30 *1000,
	}


	configClient,err = clients.CreateConfigClient(map[string]interface{}{
		"clientConfig": clientConfig,
	})
	if err != nil {
		return err
	}

	v = viper.New()
	v.SetConfigType("yaml")

	for _,key := range keys{
		//Only One Init
		content,err := configClient.GetConfig(vo.ConfigParam{
			DataId: key.Id,
			Group: key.Group,
		})
		if err != nil {
			return err
		}
		if err := v.MergeConfig(strings.NewReader(content)); err != nil {
			return err
		}

	}
	return nil
}
