package txmsg

type Config struct {
	EtcdHosts       []string `yaml:"EtcdHosts" json:"etcdHosts"`             // etcd地址
	RocketMQAddrs   []string `yaml:"RocketMQAddrs" json:"rocketMQAddrs"`     // rocketMQ消息队列地址
	DeliveryWorkNum int      `yaml:"DeliveryWorkNum" json:"deliveryWorkNum"` // 投递主机数
}
