/*
Navicat MySQL Data Transfer

Source Server         : localhost
Source Server Version : 50722
Source Host           : localhost:3306
Source Database       : stock

Target Server Type    : MYSQL
Target Server Version : 50722
File Encoding         : 65001

Date: 2022-11-08 20:39:00
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for stock_basic
-- ----------------------------
DROP TABLE IF EXISTS `stock_basic`;
CREATE TABLE `stock_basic` (
  `code_str` varchar(255) COLLATE utf8_unicode_ci NOT NULL COMMENT '股票代码000001.XSHE 688799.XSHG',
  `display_name` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL COMMENT '中文名称',
  `name` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL COMMENT '缩写简称',
  `start_date` datetime DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '上市日期',
  `end_date` datetime DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '退市日期，如果没有退市则为2200-01-01',
  `type` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL COMMENT 'stock',
  PRIMARY KEY (`code_str`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
