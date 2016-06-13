package com.hll;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Created by hll on 2016/6/12.
 */
public class PatternTest {
  public static void main(String[] args) {
    try {
      String regex = ".ox";
      String input = "bfox";
      Pattern pattern = Pattern.compile(regex);
      Matcher matcher = pattern.matcher(input);
      while (matcher.find()) {
        System.out.println("Located ["
            + matcher.group() + "] starting at "
            + matcher.start() + " and ending at " + (matcher.end() - 1));
      }
    } catch (PatternSyntaxException e) {
      e.printStackTrace();
    }
  }

}
