# gohive


本库是使用golang链接hiveserver2的库。

go通过hive的thrift rpc接口链接hiveserver2

实现了同步查询，异步查询等接口，并简化调用，优化性能，客户端不用处理复杂的thrift接口传输对象。

基于原有实现，目前支持SASL（目前支持plain，理论上后续可以支持kerbors）和NOSASL连接，支持thrift版本为0.9.3，已验证0.10和0.13由于binary_Protocol改动的问题，会导致NOSASL连接异常

func Connect() {
   host:=net.JoinHostPort("127.0.0.1", strconv.Itoa(100))
	conn, err := gohive.ConnectWithSasl(host, "hive", "hive", gohive.DefaultOptions)
	// conn, err := gohive.ConnectWithUser(host, "hive", "hive", gohive.DefaultOptions)
	if err != nil {
		fmt.Println(err)
	}
	c, err := conn.SimpleQuery(" show tables")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(c)//	[map[tab_name:datate]]
	defer conn.Close()
}

具体说明及用例会在代码架构优化后更新


