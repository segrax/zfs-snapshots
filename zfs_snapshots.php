#!/usr/local/bin/php
<?php

date_default_timezone_set('Australia/Melbourne');

define('ZFS_SNAP_CONF', 'zfs_snapshots.xml');
define('ZFS_BINARY',    '/sbin/zfs');
define('ZPOOL_BINARY',  '/sbin/zpool');

define('ZPOOL_SCRUB',   	'scrub');

define('ZFS_SNAP_ADD',  'snapshot');
define('ZFS_SNAP_REM',  'destroy');
define('ZFS_SNAP_LIST', 'list -t snapshot');

define('ZFS_SNAP_OUT', 	'NAME                                     USED  AVAIL  REFER  MOUNTPOINT');

define('SNAP_HOUR', 	'hourly-%Y-%m-%d_%H_%M_%S');
define('SNAP_DAY',  	'daily-%Y-%m-%d');
define('SNAP_WEEK', 	'weekly-%Y-%W');
define('SNAP_MONTH',	'monthly-%Y-%m');
define('SNAP_YEAR', 	'yearly-%Y');

define('DEBUG', true);

define('ZFS_SNAP_REVISION', 	 '$Revision$');
define('ZFS_SNAP_REVISION_DATE', '$Date$');


main($argc, $argv);

function main($argc, array $argv) { 

    $rev = substr( ZFS_SNAP_REVISION, 11, -2 );
    $rev .= ' @ ' . substr( ZFS_SNAP_REVISION_DATE, 7, 19 );
    
    $snapver = "zfs-snapshots (svn revision: $rev)\n\n";
    echo $snapver;
    
    $snaps = new cSnapshots( ZFS_SNAP_CONF);
    
    $snaps->zfsSnapshotRemoveOld();
    $snaps->zfsSnapshotCreateNew();
    $snaps->poolProcess();
}

class cFilesystem {
    public $_ID;
    public $_Name;
    public $_Recursive;
}

class cPool {
    public $_Name;    
    public $_TimeFormat;
    public $_Time;
    
}
        
class cTime {
    public $_Name;
    public $_ZfsTag;
    public $_Time;        // Time of execution
    public $_Keep;        // Number of snapshots to keep
    public $_TimeFormat;
   
    public $_Filesystems = array();
    public $_Snapshots = array();
}

class cZfsSnapshot {
    public $_Dataset;
    public $_Snapshot;
    public $_Timestamp;
    
    public function __construct() {
        
        $this->_Timestamp = time();
    }
}

class cSnapshots {
    private $_Filesystems = array();
    private $_Times = array();
    private $_ZfsOutput = array();
    private $_ZfsSnapshots = array();
    private $_Zpools = array();
    private $_ZpoolOutput = array();
    
    public function __construct( $pConfig ) {

        $this->configLoad( $pConfig );
        $this->zfsSnapshotLoad( );
       
        foreach( $this->_Times as &$time ) {

            foreach( $time->_Filesystems as $fs ) {

                $this->ZfsSnapshotProcessTime( $fs , $time );
            }
        }
        
        unset( $time );
    }
    
    /*
     * Find a filesystem, by its ID
     */
    private function &findFilesytemByID( $pFs ) {
        
        foreach( $this->_Filesystems as $fs ) {

            if( (string) $fs->_ID === (string) $pFs)
                return $fs;
        }

        return false;
    }
    
    /*
     * Find a filesystem, by its Name
     */
    private function &findFilesytemByName( $pName ) {
        
        foreach( $this->_Filesystems as $fs ) {

            if( (string) $fs->_Name === (string) $pName)
                return $fs;
        }

        return false;
    }
    
    private function timeLoad( $pTime, $pDefaultTime, $pDefaultKeep, $pDefaultTimeFormat ) {
        $time = new cTime();

        $time->_Name = $pTime->getName();
        
        $zfsTag = 'SNAP_' . strtoupper( $time->_Name );
        
        if( !defined( $zfsTag ))
            return;
            
        $time->_ZfsTag = constant($zfsTag);
        
        if( isset( $pTime->attributes()->format ))
            $time->_TimeFormat = $pTime->attributes()->format;
        else
            $time->_TimeFormat = $pDefaultTimeFormat;
            
        // Time 
        if( isset( $pTime->attributes()->time ))
            $time->_Time = strptime( (string) $pTime->attributes()->time , $time->_TimeFormat );
        else 
            $time->_Time = strptime( $pDefaultTime, $time->_TimeFormat );
        
        // Snapshots to keep
        if( isset( $pTime->attributes()->keep ))
            $time->_Keep = $pTime->attributes()->keep;
        else
            $time->_Keep = $pDefaultKeep;


            
        // Loop for Filesystem specified in this time
        foreach( $pTime as $fs ) {

            $name = $fs->getName();

            if( ($FS = $this->findFilesytemByID( $name )) === false )
                continue;
                
            $time->_Filesystems[ $name ] = $FS;
        }
        
        // 
        $this->_Times[$pTime->getName()] = $time;
    }
    
    private function configLoad( $pConfig ) {
        $this->_Filesystems = array();
        
        
        $config = simplexml_load_file( $pConfig );
        
        // 
        foreach( $config->fs->children() as $filesystem ) {
            
            $fs = new cFilesystem();
            
            $fs->_ID = trim( (string) $filesystem->attributes()->id );
            $fs->_Name = trim( (string) $filesystem->attributes()->name );
            $fs->_Recursive = false;
            
            if( strtolower((string)$filesystem->attributes()->recursive) === 'yes' )
                $fs->_Recursive = true;

            $this->_Filesystems[] = $fs;
        }
        
        foreach( $config->zpools->children() as $pool ) {
            
            $Pool = new cPool();
            $Pool->_Name = $pool->getName();
            //<scrub format="%H-%M %d" time="02:00 01"
            
            $Pool->_TimeFormat = (string) $pool->scrub->attributes()->format;
            
            $Pool->_Time = strptime( (string) $pool->scrub->attributes()->time, $Pool->_TimeFormat );

            $this->_Zpools[] = $Pool;
        }
        
        // Default Time/Keep settings
        $time = $config->time->attributes()->time;
        $keep = $config->time->attributes()->keep;
        $format = $config->time->attributes()->format;
        
        // Load the time nodes
        foreach( $config->time->children() as $key => $node ) {
           
            $this->timeLoad( $node, $time, $keep, $format );
        }
    }
    
    public function poolProcess() {
        
        foreach( $this->_Zpools as $pool ) {
            
            $date = strftime( $pool->_TimeFormat, time() );
            $now = strptime( $date, $pool->_TimeFormat );

            if( $pool->_Time === $now ) {
                
                if( $this->zfsScrubStatus( $pool ) === false )
                    $this->zfsScrubStart( $pool );
            }
        }
    }
    
    private function snapMake( $pFs, $pLatest, $pTimeDiff, $pFormat ) {
        
        if ( ($pLatest === null) || (time() >= strtotime( $pTimeDiff, $pLatest->_Timestamp))) {
            
            $snapshot = new cZfsSnapshot();
            
            $snapshot->_Snapshot = strftime( $pFormat , time() );
            $snapshot->_Dataset = $pFs->_Name;

            if( !($pLatest === null) && $snapshot->_Snapshot === $pLatest->_Snapshot )
                return;
                
            $this->zfsSnapshotCreate( $snapshot, $pFs->_Recursive );
        }
    }
    
    private function snapHour( $pFs, $pLatest ) {
        $this->snapMake( $pFs, $pLatest, "+1 hour", SNAP_HOUR);
    }
    
    private function snapDay( $pFs, $pLatest ) {
        $this->snapMake( $pFs, $pLatest, "+1 day", SNAP_DAY);
    }
    
    private function snapWeek( $pFs, $pLatest ) {
        $this->snapMake( $pFs, $pLatest, "+1 week", SNAP_WEEK );
    }
    
    private function snapMonth( $pFs, $pLatest ) {
        $this->snapMake( $pFs, $pLatest, "+1 month", SNAP_MONTH );
    }
    
    private function snapYear( $pFs, $pLatest ) {
        $this->snapMake( $pFs, $pLatest, "+1 year", SNAP_YEAR);
    }

    private function snapshotSort($a, $b) {
        
        if ($a->_Timestamp == $b->_Timestamp)
            return 0;
        
        return ($a->_Timestamp < $b->_Timestamp) ? -1 : 1;
    }

    public function zfsSnapshotCreateNew() {

        // Loop each time frame
        foreach( $this->_Times as $time ) {
            
            $date = strftime( $time->_TimeFormat, time() );
            $now = strptime( $date, $time->_TimeFormat );
            
            if ($time->_Time === $now ) {
                
                // Timespan Function
                $func = 'snap' . ucfirst( $time->_Name );
                            
                // No snapshots at all? 
                if( count( $time->_Snapshots ) === 0 ) {
                    
                    foreach( $time->_Filesystems as $fs ) {
                        
                        $fs = $this->findFilesytemByName( $fs->_Name );
    
                        // if timestamp is older than now - (timeframe)
                        call_user_func(array( $this, $func ), $fs, null );
                    }
                    
                    
                } else {
                    // Loop each filesystem 
                    foreach( $time->_Snapshots as $fsDataset => $snapshots ) {
                    
                        // has time elapsed since latest snapshot?
                        $fsCount = count( $snapshots );
        
                        // Number of snapshots exceeds limit for this time frame?
                        if( $fsCount <= $time->_Keep ) {
        
                            // Sort by timestamp
                            uasort( $snapshots, array($this, 'snapshotSort'));
                            
                            $latest = $snapshots[ $fsCount - 1];
        
                            $fs = $this->findFilesytemByName( $latest->_Dataset );
    
                            // if timestamp is older than now - (timeframe)
                            call_user_func(array( $this, $func ), $fs, $latest );
                        }
                    }
                }
            }
        }
        
    }
    
    public function zfsSnapshotRemoveOld() {

        // Loop each time frame
        foreach( $this->_Times as $time ) {

            // Loop each filesystem 
            foreach( $time->_Snapshots as $fsDataset => $snapshots ) {

                $fs = $this->findFilesytemByName( $fsDataset );
                
                $fsCount = count( $snapshots );
                
                // Number of snapshots exceeds limit for this time frame?
                if( $fsCount > $time->_Keep ) {
                    
                    // Sort by timestamp
                    uasort( $snapshots, array($this, 'snapshotSort'));

                    $remove = $fsCount - $time->_Keep;
                
                    // Get the snapshots that should be removed
                    $removeSnaps = array_slice( $snapshots, 0, $remove );
                    
                    $snapshots = array_slice( $snapshots, $remove);
                    
                    //
                    foreach( $removeSnaps as $snapshot ) {

                        $this->zfsSnapshotRemove( $snapshot, $fs->_Recursive );
                    }
                }
                
                $time->_Snapshots[ $fsDataset ] = $snapshots;
            }

        }
    }

    private function zfsExecute( $pZfsAction, $pOptions = array() ) {
        $this->_ZfsOutput = array();
        
        if( !is_array( $pOptions ) )
            $pOptions = array($pOptions);
        
        $options = implode( ' ', $pOptions );
        
        $args = array(	ZFS_BINARY,
                        $pZfsAction, 
                        $options,
                        '2>&1'
                        );
       
        $errorcode = 0;

        exec( implode(' ', $args), $this->_ZfsOutput, $errorcode);
       
        return $errorcode;
    }
    
    private function zPoolExecute( $pZpoolAction, $pOptions = array() ) {
        $this->_ZpoolOutput = array();
       
        if( !is_array( $pOptions ) )
            $pOptions = array($pOptions);
        
        $options = implode( ' ', $pOptions );
         
        $args = array(	ZPOOL_BINARY,
                        $pZpoolAction, 
                        $options,
                        '2>&1'
                        );
        $errorcode = 0;

        exec( implode(' ', $args), $this->_ZpoolOutput, $errorcode);
       
        return $errorcode;
    }
    
    private function zfsScrubStart( $pPool ) {
        
        if( DEBUG === true )
            echo "zpool scrub {$pPool->_Name}\n";
        else
            ;//$this->zPoolExecute( ZPOOL_SCRUB, $pPool );
    }
    
    private function zfsScrubStatus( $pPool ) {
        
        //if( DEBUG === true )
        echo "zpool status {$pPool->_Name}\n";
        //else
        $this->zPoolExecute( ZPOOL_STATUS, $pPool->_Name );
        
        $out = '';
        
        while( strpos($out,' scan: scrub in progress since ') === false ) {
            
            if( count( $this->_ZpoolOutput ) === 0 )
                return false;
                
            $out = array_shift( $this->_ZpoolOutput );
        }

        return true;
        /*
         * 
         *    pool: vader
             state: ONLINE
             scan: scrub in progress since Thu Dec  1 23:05:58 2011
                4.36M scanned out of 3.01T at 744K/s, (scan is slow, no estimated time)
                0 repaired, 0.00% done


         *    pool: syko
             state: ONLINE
            status: The pool is formatted using an older on-disk format.  The pool can
                    still be used, but some features are unavailable.
            action: Upgrade the pool using 'zpool upgrade'.  Once this is done, the
                    pool will no longer be accessible on older software versions.
             scan: scrub repaired 0 in 12h33m with 0 errors on Thu Dec  1 18:03:49 2011
            config:
            
                    NAME           STATE     READ WRITE CKSUM
                    syko           ONLINE       0     0     0
                      raidz1-0     ONLINE       0     0     0
                        gpt/disk0  ONLINE       0     0     0
                        gpt/disk1  ONLINE       0     0     0
                        gpt/disk2  ONLINE       0     0     0
                        gpt/disk3  ONLINE       0     0     0
            
            errors: No known data errors

         */
    }
    
    private function zfsSnapshotLoad() {
        $this->_ZfsSnapshots = array();
        
        // Execute zfs
        $this->zfsExecute( ZFS_SNAP_LIST );
        
        // Remove header
        if( array_shift($this->_ZfsOutput) !== ZFS_SNAP_OUT )
            return false;
        
        // 
        foreach( $this->_ZfsOutput as $output  ) {
            
            $pos = strpos( $output, '@' );
            
            $snapshot = new cZfsSnapshot();
            
            // Pool/Dataset name
            $snapshot->_Dataset = substr( $output, 0, $pos );
            
            // Skip the '@'
            $pos += 1;
            // Find the first space after the start of the snapshot name, and subtract the start to get the length
            $finish = strpos( $output, ' ', $pos ) - $pos;
            
            // get snapshot name
            $snapshot->_Snapshot = substr( $output, $pos, $finish);

            // 
            $this->_ZfsSnapshots[$snapshot->_Dataset][] = $snapshot;
        }

        return true;
    }
    
    public function zfsSnapshotCreate( $pSnapshot, $pRecursive ) {
        $Flags = '';
                
        if( $pRecursive === true )
            $Flags .= '-r ';
        
        if( DEBUG === true )
            echo "zfs snapshot $Flags{$pSnapshot->_Dataset}@{$pSnapshot->_Snapshot}\n";
        else 
            //$this->zfsExecute( ZFS_SNAP_ADD, "$Flags {$pSnapshot->_Dataset}@{$pSnapshot->_Snapshot}" );
            ;
    }
    
    private function zfsSnapshotRemove( $pSnapshot, $pRecursive ) {
        $Flags = '';
                
        if( $pRecursive === true )
            $Flags .= '-r ';
            
        if( DEBUG === true )
            echo "zfs destroy $Flags{$pSnapshot->_Dataset}@{$pSnapshot->_Snapshot}\n";
        else 
            //$this->zfsExecute( ZFS_SNAP_REM, "{$pSnapshot->_Dataset}@{$pSnapshot->_Snapshot}" );
            ;
    }
    
    private function ZfsSnapshotProcessTime( $pFilesystem, &$pTime ) {
        //$pTime->_Snapshots[ $pFilesystem->_Name ] = array();

        foreach( $this->_ZfsSnapshots[ $pFilesystem->_Name ] as $snap ) {
            
            $date = array();
            
            // Does this snapshot match the zfs tag?
            if( ($date = strptime( $snap->_Snapshot, $pTime->_ZfsTag )) === false)
                continue;

            // weekly bug (strftime doesnt work with %W, its not implemented in LIBC, atleast on freebsd)
            if( strpos( $snap->_Snapshot, 'weekly-' ) !== false ) {
                
                // weekly-year-wk
                $date = explode( '-', $snap->_Snapshot );

                $snap->_Timestamp = strtotime("{$date[1]}W{$date[2]}");

            } else {
            

                $snap->_Timestamp = mktime( $date['tm_hour'], $date['tm_min'], $date['tm_sec'], 
                                            $date['tm_mon'], $date['tm_mday'], $date['tm_year'] + 1900 );
            }
            
                                     
            $pTime->_Snapshots[ $pFilesystem->_Name][] = $snap;
        }
    }
}

