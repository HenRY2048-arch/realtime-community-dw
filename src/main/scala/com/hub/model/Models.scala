package com.hub.model

case class Event(
  event_id: String,
  user_id: Long,
  event_type: String,
  ts: String,
  session_id: String,
  platform: String,
  item_id: Option[Long] = None,
  duration_sec: Option[Double] = None,
  target_user_id: Option[Long] = None,
  comment_text: Option[String] = None,
  content_len: Option[Int] = None
)

case class ItemInfo(
  item_id: Long,
  category: String,
  content_len: Int
)

case class UserProfile(
  user_id: Long,
  interest_tags: String,
  level: Int
)

case class CheatEvent(
  event_id: String,
  user_id: Long,
  item_id: Option[Long],
  event_type: String,
  ts: String,
  is_cheat_A1: Boolean,
  is_cheat_A2: Boolean = false
)





