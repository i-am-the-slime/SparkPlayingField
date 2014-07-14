package org.menthal

import org.joda.time.DateTime
import org.menthal.model.events._
import org.menthal.model.events.adapters.PostgresDump
import org.scalatest.{FlatSpec, Matchers, BeforeAndAfterAll}

/**
 * Created by mark on 13.07.14.
 */
class PostgresDumpSpec extends FlatSpec with Matchers with BeforeAndAfterAll{
  val windowStateChange1 = WindowStateChanged("WhatsApp","com.whatsapp","[WhatsApp]")
  "getEventDataType" should "parse type 32 (WindowStateChange)" in {
    val edt = PostgresDump.getEventDataType("32", "\"[\\\\\"WhatsApp\\\\\",\\\\\"com.whatsapp/com.whatsapp.Conversation\\\\\",\\\\\"[WhatsApp]\\\\\"]\"")
    edt.get shouldBe windowStateChange1
  }
  it should "parse type 132 (WindowStateChangeBasic)" in {
    val edt = PostgresDump.getEventDataType("132", "\"[\\\\\"WhatsApp\\\\\",\\\\\"com.whatsapp/com.whatsapp.Conversation\\\\\",\\\\\"[WhatsApp]\\\\\"]\"")
    edt.get shouldBe windowStateChange1
  }
  it should "parse type 1000 (SMSReceived)" in {
    val edt = PostgresDump.getEventDataType("1000", "\"[\\\\\"6fa882592487c294b76b86b315ac9276bbcb924b93af8e40f73fde9044c23850dd20fe25068b5ef9156480c9b7fe63ff67b25ba984331cc26fc2658bd2382e8d\\\\\",\\\\\"135\\\\\"]\"")
    val result = SmsReceived("6fa882592487c294b76b86b315ac9276bbcb924b93af8e40f73fde9044c23850dd20fe25068b5ef9156480c9b7fe63ff67b25ba984331cc26fc2658bd2382e8d", 135)
    edt.get shouldBe result
  }
  it should "parse type 1001 (SMSSent)" in {
    val edt = PostgresDump.getEventDataType("1001", "\"[\\\\\"676962b809fac8b6cb64113f6ee3bc594ca3e7b59cb35863c15dd69f668b7763131e0c9a7708f5d9201e1e64783ac6eeb14c36b382bf7da14042575c54230f46\\\\\",\\\\\"125\\\\\"]\"")
    val result = SmsSent("676962b809fac8b6cb64113f6ee3bc594ca3e7b59cb35863c15dd69f668b7763131e0c9a7708f5d9201e1e64783ac6eeb14c36b382bf7da14042575c54230f46", 125)
    edt.get shouldBe result
  }
  it should "parse type 1002 (CallReceived)" in {
    val edt = PostgresDump.getEventDataType("1002", "\"[\\\\\"613a016635fc407c1b3a1b6122f2469aac080a0b3ab37110e0d92d2be5d84acda400260ffc7fc771b16af49588babd2f326eb61c5ad903a499693ba36a9e5651\\\\\",\\\\\"1369854866723\\\\\",\\\\\"64\\\\\"]\"")
    val result = CallReceived("613a016635fc407c1b3a1b6122f2469aac080a0b3ab37110e0d92d2be5d84acda400260ffc7fc771b16af49588babd2f326eb61c5ad903a499693ba36a9e5651", 1369854866723L, 64)
    edt.get shouldBe result
  }
  it should "parse type 1003 (CallOutgoing)" in {
    val edt = PostgresDump.getEventDataType("1003", "\"[\\\\\"613a016635fc407c1b3a1b6122f2469aac080a0b3ab37110e0d92d2be5d84acda400260ffc7fc771b16af49588babd2f326eb61c5ad903a499693ba36a9e5651\\\\\",\\\\\"1369852973032\\\\\",\\\\\"43\\\\\"]\"")
    val result = CallOutgoing("613a016635fc407c1b3a1b6122f2469aac080a0b3ab37110e0d92d2be5d84acda400260ffc7fc771b16af49588babd2f326eb61c5ad903a499693ba36a9e5651", 1369852973032L, 43)
    edt.get shouldBe result
  }
  it should "parse type 1004 (CallMissed)" in {
    val edt = PostgresDump.getEventDataType("1004", "\"[\\\\\"dfc0d0ca2c2e8e5ea10a3f4b1f941baaf384ee5b3a7d6998a46e88e70cb9ad32da1c743eb4dc88528d9a4c0c53a34e809a34077c4af750bf3d93eee5fdaa297c\\\\\",\\\\\"1368721312893\\\\\",\\\\\"0\\\\\"]\"")
    val result = CallMissed("dfc0d0ca2c2e8e5ea10a3f4b1f941baaf384ee5b3a7d6998a46e88e70cb9ad32da1c743eb4dc88528d9a4c0c53a34e809a34077c4af750bf3d93eee5fdaa297c", 1368721312893L)
    edt.get shouldBe result
  }
  it should "parse type 1005 (ScreenOn)" in {
    val edt = PostgresDump.getEventDataType("1005", "[]")
    edt.get shouldBe ScreenOn()
  }
  it should "parse type 1006 (ScreenOff)" in {
    val edt = PostgresDump.getEventDataType("1006", "[]")
    edt.get shouldBe ScreenOff()
  }
  it should "parse type 1007 (Localisation)" in {
    val edt = PostgresDump.getEventDataType("1007", "\"[\\\\\"network\\\\\",\\\\\"1124.0\\\\\",\\\\\"7.0958195\\\\\",\\\\\"50.7465128\\\\\"]\"")
    val result = Localisation("network", 1124.0f, 7.0958195, 50.7465128)
    edt.get shouldBe result
  }
  it should "parse type 1008 (AppList)" in {
    val edt = PostgresDump.getEventDataType("1008", "\"[{\\\\\"appName\\\\\":\\\\\"Calculator\\\\\",\\\\\"pkg\\\\\":\\\\\"com.android.calculator2\\\\\"},{\\\\\"appName\\\\\":\\\\\"Contacts\\\\\",\\\\\"pkg\\\\\":\\\\\"com.android.contacts\\\\\"}]\"")
    val list = List( AppListItem("com.android.calculator2", "Calculator"),
      AppListItem("com.android.contacts", "Contacts"))
    edt.get shouldBe AppList(list)
  }
  ignore should "parse type 1009 (AppInstall)" in {
    val edt = PostgresDump.getEventDataType("1009", "\"[\\\\\"Nyx\\\\\",\\\\\"com.menthal.nyx\\\\\"]\"")
//    edt.get shouldBe AppInstall("Nyx", "com.menthal.nyx")
  }
  it should "parse type 1010 (AppRemoval)" in {
    val edt = PostgresDump.getEventDataType("1010", "\"[\\\\\"Nyx\\\\\",\\\\\"com.menthal.nyx\\\\\"]\"")
    edt.get shouldBe AppRemoval("Nyx", "com.menthal.nyx")
  }
  it should "parse type 1011 (Mood)" in {
    val edt = PostgresDump.getEventDataType("1011", "\"[\\\\\"3.0\\\\\",\\\\\"\\\\\"]\"")
    edt.get shouldBe Mood(3f)
  }
  it should "parse type 1012 (PhoneBoot)" in {
    val edt = PostgresDump.getEventDataType("1012", "[]")
    edt.get shouldBe PhoneBoot()
  }
  it should "parse type 1013 (PhoneShutdown" in {
    val edt = PostgresDump.getEventDataType("1013", "[]")
    edt.get shouldBe PhoneShutdown()
  }
  it should "parse type 1014 (ScreenUnlock)" in {
    val edt = PostgresDump.getEventDataType("1014", "[]")
    edt.get shouldBe ScreenUnlock()
  }
  it should "parse type 1017 (DreamingStarted)" in {
    val edt = PostgresDump.getEventDataType("1017", "[]")
    edt.get shouldBe DreamingStarted()
  }
  it should "parse type 1018 (DreamingStopped)" in {
    val edt = PostgresDump.getEventDataType("1018", "[]")
    edt.get shouldBe DreamingStopped()
  }
  it should "parse type 1019 (WhatsAppSent)" in {
    val edt = PostgresDump.getEventDataType("1019", "\"[\\\\\"719b886597f64e0c48c087848c676fde59a5a61cd3ee5940461ce3b1c0d9b602b706427d532a24badf32ec499de09a8098ad8e0e56aa0bfef1facea603ac5a09\\\\\",17,1]\"")
    val hash = "719b886597f64e0c48c087848c676fde59a5a61cd3ee5940461ce3b1c0d9b602b706427d532a24badf32ec499de09a8098ad8e0e56aa0bfef1facea603ac5a09"
    val result = WhatsAppSent(hash, 17, isGroupMessage = true)
    edt.get shouldBe result
  }
  it should "parse type 1020 (WhatsAppReceived)" in {
    val edt = PostgresDump.getEventDataType("1020", "\"[\\\\\"532b6bbd7b36cf5b6483c1e47e400c70d49937f00a7d4f9f5d464f57ca9130aebf94b2c1f103c2bf367ef7fbfe63a576479e911ba2032354df5351bf35befd5f\\\\\",10,1]\"")
    val hash = "532b6bbd7b36cf5b6483c1e47e400c70d49937f00a7d4f9f5d464f57ca9130aebf94b2c1f103c2bf367ef7fbfe63a576479e911ba2032354df5351bf35befd5f"
    val result = WhatsAppReceived(hash, 10, isGroupMessage = true)
    edt.get shouldBe result
  }
  it should "parse type 1021 (Device Features)" in {
    //TODO: Find an example from a dump to test this.
  }
  it should "parse type 1022 (Menthal App Action)" in {
    //TODO: Find an example from a dump to test this.
  }
  it should "parse type 1023 (Timezone)" in {
    //TODO: Find an example from a dump to test this.
  }
  it should "parse type 1025 (Traffic Data)" in {
    //TODO: Find an example from a dump to test this.
  }
  it should "parse type 1032 (App Session)" in {
    //TODO: Find an example from a dump to test this.
  }
  it should "parse type 1100 (Questionnaire)" in {
    //TODO: Find an example from a dump to test this.
    /*
    val edt = PostgresDump.getEventDataType("1100", "")
    val answers = List(

    )
    val result = Questionnaire(answers)
    edt.get shouldBe result
    */
  }
  it should "fail on unknown numbers" in {
    val edt = PostgresDump.getEventDataType("4093888", "bla")
    edt shouldBe None
  }

  "tryToParseLine" should "parse WindowStateChangedEvents" in {
    val line = "79822117\t22812\t2014-01-22 22:44:04.719+01\t32\t\"[\\\\\"WhatsApp\\\\\",\\\\\"com.whatsapp/com.whatsapp.Conversation\\\\\",\\\\\"[WhatsApp]\\\\\"]\""
    val result = PostgresDump.tryToParseLineFromDump(line)
    val expected = Event(79822117, 22812, DateTime.parse("2014-01-22T22:44:04.719+01").getMillis ,windowStateChange1)
    result.get shouldBe expected
  }
}
