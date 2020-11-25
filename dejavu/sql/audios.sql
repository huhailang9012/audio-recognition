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

 Date: 25/11/2020 14:05:02
*/


-- ----------------------------
-- Table structure for audios
-- ----------------------------
DROP TABLE IF EXISTS "public"."audios";
CREATE TABLE "public"."audios" (
  "audio_id" char(32) COLLATE "pg_catalog"."default" NOT NULL,
  "audio_name" varchar(250) COLLATE "pg_catalog"."default" NOT NULL,
  "fingerprinted" int2 DEFAULT 0,
  "file_sha1" bytea,
  "total_hashes" int4 NOT NULL DEFAULT 0,
  "date_created" timestamp(6) NOT NULL DEFAULT now(),
  "date_modified" timestamp(6) NOT NULL DEFAULT now()
)
;

-- ----------------------------
-- Primary Key structure for table audios
-- ----------------------------
ALTER TABLE "public"."audios" ADD CONSTRAINT "pk_audios_audio_id" PRIMARY KEY ("audio_id");
