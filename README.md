# zfs-snapshots
A quick script to take hourly, daily, monthly, weekly, yearly snapshots on a ZFS filesystem

#### Setup

##### Cronjob
```
* * * * * /snapshots/zfs_snapshots.php >/dev/null 2>&1
```

##### Example Config
```
 zfs_snapshots.xml
<?xml version='1.0' standalone='yes'?>

<zfs>
        <zpools>
                <syko>
                        <scrub format="%H:%M %d" time="02:40 01"/>
                </syko>
        </zpools>

        <fs>
                <fs id="storage" name="syko/storage" recursive="yes"/>
                <fs id="home"    name="syko/home"    recursive="yes"/>
                <fs id="jails"   name="syko/jails"   recursive="yes"/>
                <fs id="vms"     name="syko/vms"     recursive="yes"/>
                <fs id="fbsd"    name="syko/os/fbsd9" resursive="yes"/>
        </fs>

        <time time="22:08" format="%H:%M" keep="1">

                <hour keep="3" diff="+1 hour" time="00" format="%M" snapshot="hourly-%Y-%m-%d_%H_%M">
                        <home/>
                </hour>

                <day keep="7" diff="+1 day" snapshot="daily-%Y-%m-%d">
                        <storage/>
                        <home/>
                        <jails/>
                        <video/>
                        <vms/>
                        <fbsd/>
                </day>

                <week keep="5" diff="+1 week" snapshot="weekly-%Y-%W">
                        <storage/>
                        <home/>
                        <jails/>
                        <video/>
                        <vms/>
                        <fbsd/>
                </week>

                <month keep="12" diff="+1 month" snapshot="monthly-%Y-%m">
                        <storage/>
                        <home/>
                        <jails/>
                        <vms/>
                        <fbsd/>
                </month>
        </time>

</zfs>
```
