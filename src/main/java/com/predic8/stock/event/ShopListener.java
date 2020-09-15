package com.predic8.stock.event;

import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.predic8.stock.model.Stock;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;


@Service
public class ShopListener {
	private final ObjectMapper mapper;
	private final Map<String, Stock> repo;
	private final NullAwareBeanUtilsBean beanUtils;

	public ShopListener(ObjectMapper mapper, Map<String, Stock> repo, NullAwareBeanUtilsBean beanUtils) {
		this.mapper = mapper;
		this.repo = repo;
		this.beanUtils = beanUtils;
	}

	@KafkaListener(topics = "shop")
	public void listen(Operation op) throws Exception {
		System.out.println("op = " + op);

		if(op.getBo().equals("article")) {
			Stock stock = mapper.treeToValue(op.getObject(), Stock.class);
			if(op.getAction().equals("upsert")) {
				repo.put(stock.getUuid(), stock);
			} else if(op.getAction().equals("remove")) {
				repo.remove(stock.getUuid());
			}
		}
	}
}