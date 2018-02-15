# coding: utf-8

import pip

__title__ = 'pandas-cassandra-s3'
__version__ = '0.1'
__author__ = 'Xuzhou Qin'
__author_email__ = 'xuzhou.qin@jcdecaux.com'
__description__ = ''

__dependencies__ = sorted(["%s==%s" % (i.key, i.version)
                           for i in pip.get_installed_distributions()])
