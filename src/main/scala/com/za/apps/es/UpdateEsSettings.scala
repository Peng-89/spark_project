package com.za.apps.es

import java.net.InetAddress

import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress

object UpdateEsSettings {

  def main(args: Array[String]): Unit = {
    //val client = ESUtils.getClient();
    val settings = Settings.settingsBuilder()
      //可以更新的配置还有很多，见elasticsearch官网
      .put("number_of_replicas", 3).build();
    val client = TransportClient.builder().build().addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.18.67.111"), 9300));
    val updateSettingsResponse = client.admin().indices().prepareUpdateSettings("megacorp").setSettings(settings).execute().actionGet()
  }
}
