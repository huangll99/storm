package com.hll.log;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by hll on 2016/6/12.
 */
public class ApacheLogSplitter {
  public Map<String, Object> logSplitter(String apacheLog) {
    String logEntryLine = apacheLog;
    String logEntryPattern = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:-]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"";
    Pattern p = Pattern.compile(logEntryPattern);
    Matcher matcher = p.matcher(logEntryLine);
    Map<String, Object> logMap = new HashMap<>();
    if (!matcher.matches() || 9 != matcher.groupCount()) {
      System.err.println("Bad log entry (or problem with RE?):");
      System.err.println(logEntryLine);
      return logMap;
    }
    logMap.put("ip", matcher.group(1));
    logMap.put("dateTime", matcher.group(4));
    logMap.put("request", matcher.group(5));
    logMap.put("response", matcher.group(6));
    logMap.put("bytesSent", matcher.group(7));
    logMap.put("referrer", matcher.group(8));
    logMap.put("useragent", matcher.group(9));
    return logMap;
  }

  public static void main(String[] args) {
    ApacheLogSplitter splitter = new ApacheLogSplitter();
    String text = "4.19.162.143 - - [4-03-2011:06:20:31 -0500] \"GET / HTTP/1.1\" 200 864 \"http://www.adeveloper.com/resource.html\" \"Mozilla/5.0 (Windows; U; WindowsNT 5.1; hu-HU; rv:1.7.12) Gecko/20050919 Firefox/1.0.7\"";

    Map<String, Object> map = splitter.logSplitter(text);
    System.out.println(map);
  }
}
