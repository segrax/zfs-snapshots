#!/usr/local/bin/php
<?php

define('ZFS_SNAP_CONF', 'zfs_snapshots.xml');
define('ZFS_BINARY',    '/sbin/zfs');

define('ZFS_SNAP_ADD',  'snapshot');
define('ZFS_SNAP_REM',  'destroy');
define('ZFS_SNAP_LIST', 'list -t snapshot');

define('ZFS_SNAP_OUT', 	'NAME                                     USED  AVAIL  REFER  MOUNTPOINT');

define('SNAP_HOUR', 	'hourly-');
define('SNAP_DAY',  	'daily-%Y-%m-%d');
define('SNAP_WEEK', 	'weekly-%Y-%W');
define('SNAP_MONTH',	'monthly-%Y-%m');
define('SNAP_YEAR', 	'yearly-%Y');

define('DEBUG', true);

define('ZFS_SNAP_REVISION', 	 '$Revision$');
define('ZFS_SNAP_REVISION_DATE', '$Date$');

date_default_timezone_set('Australia/Melbourne');

main($argc, $argv);

function main($argc, array $argv) { 

    $rev = substr( ZFS_SNAP_REVISION, 11, -2 );
    $rev .= ' @ ' . substr( ZFS_SNAP_REVISION_DATE, 7, 19 );
    
    $snapver = "zfs-snapshots (svn revision: $rev)\n\n";
    echo $snapver;
    
    $snaps = new cSnapshots( ZFS_SNAP_CONF);
    
    $snaps->zfsSnapshotRemoveOld();
    $snaps->zfsSnapshotCreateNew();
}

class cFilesystem {
    public $_ID;
    public $_Name;
    public $_Recursive;
}

class cTime {
    public $_Name;
    public $_ZfsTag;
    public $_Time;        // Time of execution
    public $_Keep;        // Number of snapshots to keep
    
    
    public $_Seconds;    // Number of seconds between snapshots
    
    public $_Filesystems = array();
    public $_Snapshots = array();
}

class cZfsSnapshot {
    public $_Dataset;
    public $_Snapshot;
    public $_Timestamp;
}

class cSnapshots {
    private $_Filesystems = array();
    private $_Times = array();
    private $_ZfsOutput = array();
    private $_ZfsSnapshots = array();
    
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
    
    private function &findFilesytem( $pFs ) {
        
        foreach( $this->_Filesystems as $fs ) {
            
            if( (string) $fs->_ID === (string) $pFs)
                return $fs;
        }

        return false;
    }
    
    private function timeLoad( $pTime, $pDefaultTime, $pDefaultKeep ) {
        $time = new cTime();

        $time->_Name = $pTime->getName();
        
        switch( $time->_Name  ) {
            case "hour":
                $zfsTag = SNAP_HOUR;
                
                break;
                
            case "day":
                $zfsTag = SNAP_DAY;
                break;
                
            case "week":
                $zfsTag = SNAP_WEEK;
                break;
                
            case "month":
                $zfsTag = SNAP_MONTH;
                break;
                
            case "year":
                $zfsTag = SNAP_YEAR;
                break;
        }

        $time->_ZfsTag = $zfsTag;
        
        // Time 
        if( isset( $pTime->attributes()->time ))
            $time->_Time = $pTime->attributes()->time;
        else 
            $time->_Time = $pDefaultTime;
        
        // Snapshots to keep
        if( isset( $pTime->attributes()->keep ))
            $time->_Keep = $pTime->attributes()->keep;
        else
            $time->_Keep = $pDefaultKeep;
        
        // Loop for Filesystem specified in this time
        foreach( $pTime as $fs ) {

            $name = $fs->getName();

            if( ($FS = $this->findFilesytem( $name )) === false )
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
            
            $fs->_ID = (string) $filesystem->attributes()->id;
            $fs->_Name = (string) $filesystem->attributes()->name;
            $fs->_Recursive = (string) $filesystem->attributes()->recursive;
            
            $this->_Filesystems[] = $fs;
        }
        
        // Default Time/Keep settings
        $time = $config->time->attributes()->time;
        $keep = $config->time->attributes()->keep;
        
        // Load the time nodes
        foreach( $config->time->children() as $key => $node ) {
           
            $this->timeLoad( $node, $time, $keep );
        }
    }

    private function snapshotSort($a, $b) {
        
        if ($a->_Timestamp == $b->_Timestamp)
            return 0;
        
        return ($a->_Timestamp < $b->_Timestamp) ? -1 : 1;
    }

    public function zfsSnapshotCreateNew() {

        // Loop each time frame
        foreach( $this->_Times as $time ) {

            // Loop each filesystem 
            foreach( $time->_Snapshots as $fsDataset => $fs ) {
            
                // has time elapsed since latest snapshot?
                $fsCount = count( $fs );
                
                // Number of snapshots exceeds limit for this time frame?
                //if( $fsCount > $time->_Keep ) {

                    // Sort by timestamp
                    uasort( $fs, array($this, 'snapshotSort'));
                    
                    $latest = $fs[ $fsCount - 1];
                    
                    
                    // if timestamp is older than now - (timeframe)
                    $latest->_Timestamp;

                //}
            }
        }
        
    }
    
    public function zfsSnapshotRemoveOld() {

        // Loop each time frame
        foreach( $this->_Times as $time ) {

            // Loop each filesystem 
            foreach( $time->_Snapshots as $fsDataset => $fs ) {

                $fsCount = count( $fs );
                
                // Number of snapshots exceeds limit for this time frame?
                if( $fsCount > $time->_Keep ) {
                    
                    // Sort by timestamp
                    uasort( $fs, array($this, 'snapshotSort'));

                    $remove = $fsCount - $time->_Keep;
                
                    // Get the snapshots that should be removed
                    $removeSnaps = array_slice( $fs, 0, $remove );

                    //
                    foreach( $removeSnaps as $snapshot ) {
                        
                        $this->zfsSnapshotRemove( $snapshot );
                    }
                }
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
           
        echo "zfs snapshot $Flags {$pSnapshot->_Dataset}@{$pSnapshot->_Snapshot}\n";
        
    }
    
    private function zfsSnapshotRemove( $pSnapshot ) {
        
        if( DEBUG === true )
            echo "zfs destroy {$pSnapshot->_Dataset}@{$pSnapshot->_Snapshot}\n";
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

