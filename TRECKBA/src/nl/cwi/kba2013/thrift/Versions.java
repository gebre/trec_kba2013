/**
 * Autogenerated by Thrift Compiler (0.8.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package nl.cwi.kba2013.thrift;


import java.util.Map;
import java.util.HashMap;
import org.apache.thrift.TEnum;

/**
 * Versions of this protocol are enumerated so that when we expand,
 * everybody can see which version a particular data file used.
 * 
 * v0_1_0 refers to the kba.thrift definition, which was before
 * Versions was included in the spec.
 */
public enum Versions implements org.apache.thrift.TEnum {
  v0_2_0(0);

  private final int value;

  private Versions(int value) {
    this.value = value;
  }

  /**
   * Get the integer value of this enum value, as defined in the Thrift IDL.
   */
  public int getValue() {
    return value;
  }

  /**
   * Find a the enum type by its integer value, as defined in the Thrift IDL.
   * @return null if the value is not found.
   */
  public static Versions findByValue(int value) { 
    switch (value) {
      case 0:
        return v0_2_0;
      default:
        return null;
    }
  }
}
