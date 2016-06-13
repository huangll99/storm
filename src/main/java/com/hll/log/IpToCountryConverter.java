package com.hll.log;

import com.maxmind.geoip.Country;
import com.maxmind.geoip.LookupService;

import java.io.IOException;

/**
 * Created by hll on 2016/6/12.
 */
public class IpToCountryConverter {
  private LookupService ls;

  public IpToCountryConverter(String pathToGeoLiteCityFile) {
    try {
      ls = new LookupService( pathToGeoLiteCityFile,LookupService.GEOIP_MEMORY_CACHE);
    } catch (IOException e) {
      throw new RuntimeException("error:"+e);
    }
  }

  public String ipToCountry(String ip) {
    Country country = ls.getCountry(ip);
    if (country == null) {
      return "NA";
    }
    if (country.getName() == null) {
      return "NA";
    }
    return country.getName();
  }

  public static void main(String[] args) {
    IpToCountryConverter c = new IpToCountryConverter("E:\\projects\\storm\\src\\main\\resources\\GeoIP.dat");
    String s = c.ipToCountry("4.20.73.32");
    System.out.println(s);
  }
}
