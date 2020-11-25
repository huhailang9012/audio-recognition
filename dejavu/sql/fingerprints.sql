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

 Date: 25/11/2020 14:05:08
*/


-- ----------------------------
-- Table structure for fingerprints
-- ----------------------------
DROP TABLE IF EXISTS "public"."fingerprints";
CREATE TABLE "public"."fingerprints" (
  "hash" bytea NOT NULL,
  "audio_id" char(32) COLLATE "pg_catalog"."default" NOT NULL,
  "offset" int4 NOT NULL,
  "date_created" timestamp(6) NOT NULL DEFAULT now(),
  "date_modified" timestamp(6) NOT NULL DEFAULT now()
)
;

-- ----------------------------
-- Indexes structure for table fingerprints
-- ----------------------------
CREATE INDEX "ix_fingerprints_hash" ON "public"."fingerprints" USING hash (
  "hash" "pg_catalog"."bytea_ops"
);

-- ----------------------------
-- Uniques structure for table fingerprints
-- ----------------------------
ALTER TABLE "public"."fingerprints" ADD CONSTRAINT "uq_fingerprints" UNIQUE ("audio_id", "offset", "hash");

-- ----------------------------
-- Foreign Keys structure for table fingerprints
-- ----------------------------
ALTER TABLE "public"."fingerprints" ADD CONSTRAINT "fk_fingerprints_audio_id" FOREIGN KEY ("audio_id") REFERENCES "public"."audios" ("audio_id") ON DELETE CASCADE ON UPDATE NO ACTION;
