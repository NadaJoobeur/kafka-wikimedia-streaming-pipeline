package com.nadajoobeur.consumer.repository;

import com.nadajoobeur.consumer.document.WikimediaEvent;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface WikimediaEventRepository extends MongoRepository<WikimediaEvent,String> {

}
