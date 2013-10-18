moosefs 监控程序
===============

moosefs 监控程序，可发送警报至email。使用前，需要修改check_mfs.py里的smtp配置和通知email地址

使用方法： ./check_mfs.py (start|stop|restart)

根据moosefs开源代码mfscgi改写(daemon.py来源网络)，首次写python，有不对或改进的地方请指出。
