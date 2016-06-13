package com.hll;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by hll on 2016/6/12.
 */
public class PatternGroupTest {
  public static void main(String[] args) {
   /* String text    =  "John writes about this, and John writes about that," +
    " and John writes about everything. "  ;
    String patterS="(John)";
    Pattern pattern = Pattern.compile(patterS);
    Matcher matcher = pattern.matcher(text);
    while (matcher.find()){
      System.out.println("found: "+matcher.group(1));
    }*/

    /*String text =
        "John writes about this, and John Doe writes about that," +
            " and John Wayne writes about everything.";
    String ps = "((John) (.+?)) ";
    Pattern pattern = Pattern.compile(ps);
    Matcher matcher = pattern.matcher(text);
    while (matcher.find()) {
      System.out.println("found: " + matcher.group(1) + " " + matcher.group(2) + " " + matcher.group(3));
    }*/

    String text="4.19.162.143 - - [4-03-2011:06:20:31 -0500] \"GET / HTTP/1.1\" 200 864 \"http://www.adeveloper.com/resource.html\" \"Mozilla/5.0 (Windows; U; WindowsNT 5.1; hu-HU; rv:1.7.12) Gecko/20050919 Firefox/1.0.7\"";
    String reg="([\\d.]+) (\\S+) (\\S+) \\[([\\w:-]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"";
    Pattern pattern = Pattern.compile(reg);
    Matcher matcher = pattern.matcher(text);
    while (matcher.find()){
      System.out.println(matcher.matches());
      System.out.println(matcher.groupCount());
      System.out.println(matcher.group(1));
      System.out.println(matcher.group(2));
      System.out.println(matcher.group(3));
      System.out.println(matcher.group(4));
      System.out.println(matcher.group(5));
      System.out.println(matcher.group(6));
      System.out.println(matcher.group(7));
      System.out.println(matcher.group(8));
      System.out.println(matcher.group(9));
    }
  }
}
