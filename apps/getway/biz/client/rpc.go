package client

func InitRpcClient() {
	initUserGrpc()
	newActivityRpc()
	newMarketRpc()
	newSeckillClient()
}
