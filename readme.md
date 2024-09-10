WsProducer -WSLog-> LogActor -TradeLog-> TradeActor
                             -TradeLog-> DBActor

WSLog{data: string, ts: i32}
TradeLog{pair: string, delta: i32}
