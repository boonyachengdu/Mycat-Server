

DROP TABLE IF EXISTS `userinfo`;
CREATE TABLE `userinfo` (
  `id` int(20) NOT NULL,
  `name` varchar(50) DEFAULT NULL,
  `phone` varchar(30) DEFAULT NULL,
  `address` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


DROP TABLE IF EXISTS `processtask`;
CREATE TABLE `processtask` (
  `id` int(12) NOT NULL AUTO_INCREMENT,
  `pmethod` varchar(50) DEFAULT NULL,
  `plimit` int(20) DEFAULT NULL,
  `ptime` int(20) DEFAULT NULL,
  `systime` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;