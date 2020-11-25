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

 Date: 25/11/2020 14:05:20
*/


-- ----------------------------
-- Table structure for related_audios
-- ----------------------------
DROP TABLE IF EXISTS "public"."related_audios";
CREATE TABLE "public"."related_audios" (
  "id" char(32) COLLATE "pg_catalog"."default" NOT NULL,
  "audio_id" char(32) COLLATE "pg_catalog"."default" NOT NULL,
  "related_audio_id" char(32) COLLATE "pg_catalog"."default" NOT NULL,
  "related_audio_name" varchar(128) COLLATE "pg_catalog"."default",
  "matched_id" char(32) COLLATE "pg_catalog"."default" NOT NULL,
  "input_total_hashes" int4,
  "fingerprinted_hashes_in_db" int4,
  "hashes_matched_in_input" int4,
  "input_confidence" float4,
  "fingerprinted_confidence" float4,
  "offset" int4,
  "offset_seconds" int4,
  "file_sha1" bytea
)
;

-- ----------------------------
-- Primary Key structure for table related_audios
-- ----------------------------
ALTER TABLE "public"."related_audios" ADD CONSTRAINT "related_audios_pkey" PRIMARY KEY ("id");

-- ----------------------------
-- Foreign Keys structure for table related_audios
-- ----------------------------
ALTER TABLE "public"."related_audios" ADD CONSTRAINT "fk_related_audios_matched_id" FOREIGN KEY ("matched_id") REFERENCES "public"."matched_information" ("id") ON DELETE NO ACTION ON UPDATE NO ACTION;
ALTER TABLE "public"."related_audios" ADD CONSTRAINT "fk_related_audios_related_audio_id" FOREIGN KEY ("related_audio_id") REFERENCES "public"."audios" ("audio_id") ON DELETE NO ACTION ON UPDATE NO ACTION;
