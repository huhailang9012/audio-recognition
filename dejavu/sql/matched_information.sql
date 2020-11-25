/*
 Navicat Premium Data Transfer

 Source Server         : AR
 Source Server Type    : PostgreSQL
 Source Server Version : 100007
 Source Host           : localhost:5432
 Source Catalog        : audio_recognition
 Source Schema         : public

 Target Server Type    : PostgreSQL
 Target Server Version : 100007
 File Encoding         : 65001

 Date: 25/11/2020 14:05:13
*/


-- ----------------------------
-- Table structure for matched_information
-- ----------------------------
DROP TABLE IF EXISTS "public"."matched_information";
CREATE TABLE "public"."matched_information" (
  "id" char(32) COLLATE "pg_catalog"."default" NOT NULL,
  "audio_id" char(32) COLLATE "pg_catalog"."default" NOT NULL,
  "audio_name" varchar(128) COLLATE "pg_catalog"."default" NOT NULL,
  "audio_md5" char(32) COLLATE "pg_catalog"."default" NOT NULL,
  "total_time" float4,
  "fingerprint_time" float4,
  "query_time" float4,
  "align_time" float4,
  "date_created" char(19) COLLATE "pg_catalog"."default" NOT NULL,
  "related_key" varchar(64) COLLATE "pg_catalog"."default"
)
;

-- ----------------------------
-- Primary Key structure for table matched_information
-- ----------------------------
ALTER TABLE "public"."matched_information" ADD CONSTRAINT "matched_information_pkey" PRIMARY KEY ("id");
