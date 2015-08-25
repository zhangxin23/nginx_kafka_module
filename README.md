##声明

此模块是在https://github.com/brg-liuwei/ngx_kafka_module基础上改写，将原来的向单个broker发送
POST请求，改为可以向broker集群发送POST请求。
在此对brg_liuwei表示由衷的感谢。

##Introduction

This module is used to send post data from nginx to kafka.

##Installation

###1 install librdkafka

    git clone https://github.com/edenhill/librdkafka
    cd librdkafka
    ./configure
    make
    sudo make install

###2 compile nginx with nginx kafka module

    git clone https://github.com/zhangxin23/nginx_kafka_module
    cd /path/to/nginx
    ./configure --add-module=/path/to/nginx_kafka_module
    make
    sudo make install

###3 edit nginx.conf file

    http {

        ... ...

        kafka.broker.list  192.168.1.2:9092 192.168.1.3:9092;

        server {

            ... ...

            location = /topic_1 {
                kafka.topic  topic_1;
            }

            ... ...
        
        }

    }

###4 start nginx

###5 test

    curl localhost/topic_1 -d "hello kafka"
