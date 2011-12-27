#!/usr/local/bin/php
<?php

/**
 * zfs_snapshots :  Take snapshots on a ZFS filesystem at regular intervals, 
 * 					Delete old snapshots
 * 					Regular pool scrub
 * 
 * @author Robert Crossfield <robcrossfield@gmail.com>
 * 
 * @copyright Strobs Canardly Systems 2011
 */

// Dont actually execute commands
define('DEBUG', false);

// Default Timezone
date_default_timezone_set('Australia/Melbourne');

// Default Config
define('ZFS_SNAP_CONF', dirname(__FILE__) . '/zfs_snapshots.xml');

define('LOG_FILE', __DIR__ . '/action.log' );

// Binarys
define('ZFS_BINARY',    '/sbin/zfs');
define('ZPOOL_BINARY',  '/sbin/zpool');

// Pool Commands
define('ZPOOL_SCRUB',   'scrub');

// ZFS commands
define('ZFS_SNAP_ADD',  'snapshot');
define('ZFS_SNAP_REM',  'destroy');
define('ZFS_SNAP_LIST', 'list -t snapshot');

// Zfs snapshot list output first row
define('ZFS_SNAP_OUT', 	'USED  AVAIL  REFER  MOUNTPOINT');

// SVN Details
define('ZFS_SNAP_REVISION', 	 '$Revision$');
define('ZFS_SNAP_REVISION_DATE', '$Date$');


// Start up...
main($argc, $argv);

/**
 * Representation of a 'filesystem', a Zfs dataset node
 * 
 * Holds snapshots for this node
 */
class cFilesystem {
    public $_ID;
    public $_Name;
    public $_Recursive;
    
    public $_ZfsSnapshots = array();
}

/**
 * Representation of a zpool
 */
class cPool {
    public $_Name;    
    public $_TimeFormat;
    public $_Time;
}

/**
 * Representation of a time frame
 */
class cTime {
    public $_Name;
    public $_Time;        // Time of execution
    public $_Keep;        // Number of snapshots to keep
    public $_TimeFormat;
    public $_TimeDifference; 
    public $_SnapshotFormat;
    
    public $_Filesystems = array();
    public $_Snapshots = array();
}

/**
 * Representation of a Zfs Snapshot
 */
class cZfsSnapshot {
    public $_Dataset;
    public $_Snapshot;
    public $_Timestamp;
    
    public function __construct() {
        
        $this->_Timestamp = time();
    }
}

/**
 * The Zfs-Snapshots Controller
 */
class cSnapshots {
    private $_Filesystems = array();
    private $_Times = array();
    private $_ZfsOutput = array();
    private $_Zpools = array();
    private $_ZpoolOutput = array();
    
    /**
     * Constructor
     * 
     * @param string $pConfig XML Configuration File
     */
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
    
    /**
     * Find a filesystem, by its ID
     * 
     * @param string $pId ID of a filesystem
     * 
     * @return cFilesystem|bool Filesystem if found
     */
    private function &findFilesytemByID( $pId ) {
        
        foreach( $this->_Filesystems as $fs ) {

            if( (string) $fs->_ID === (string) $pId)
                return $fs;
        }

        return false;
    }
    
    /**
     * Find a filesystem, by its Name
     * 
     * @param string $pName Name of the filesystem
     * 
     * @return cFilesystem|bool Filesystem if found
     */
    private function &findFilesytemByName( $pName ) {
        
        foreach( $this->_Filesystems as $fs ) {

            if( (string) $fs->_Name === (string) $pName)
                return $fs;
        }

        return false;
    }
    
    /**
     * Load the time Node from the XML file
     * 
     * @param SimpleXMLElement $pTime              Current TIme Node
     * @param string           $pDefaultTime       
     * @param string           $pDefaultKeep       
     * @param string           $pDefaultTimeFormat 
     * 
     * @return void
     */
    private function timeLoad( $pTime, $pDefaultTime, $pDefaultKeep, $pDefaultTimeFormat ) {
        $time = new cTime();

        $time->_Name = $pTime->getName();
        
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

        $time->_TimeDifference = (string) $pTime->attributes()->diff;

        $time->_SnapshotFormat = (string) $pTime->attributes()->snapshot;
        
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
    
    /**
     * Load the configuration file
     * 
     * @param string $pConfig Path/Filename of the configuration file
     * 
     * @return void
     */
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
    
    /**
     * Process all pools and begin scrub(s) if required
     * 
     * @return void
     */
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
    
    /**
     * Create a snapshot, if none exist, or if the time between snapshots is elapsed
     * 
     * @param cFilesystem $pFs     The filesystem
     * @param cSnapshot   $pLatest The last available snapshot
     * @param string      $pTime   Timeframe operating on
     * 
     * @return void
     */
    private function snapshotMake( $pFs, $pLatest, $pTime ) {

        if ( ($pLatest === null) || (time() >= strtotime( $pTime->_TimeDifference, $pLatest->_Timestamp))) {

            $snapshot = new cZfsSnapshot();
            
            $snapshot->_Snapshot = strftime( $pTime->_SnapshotFormat , time() );
            $snapshot->_Dataset = $pFs->_Name;

            if( !($pLatest === null) && $snapshot->_Snapshot === $pLatest->_Snapshot )
                return;

            $this->zfsSnapshotCreate( $snapshot, $pFs->_Recursive );
        }
    }

    /**
     * Comparea the timestamps of two snapshots
     * 
     * @return int -1 if the '$pA' timestamp is less than the '$pB' timestamp
     */
    private function snapshotSort($pA, $pB) {
        
        if ($pA->_Timestamp == $pB->_Timestamp)
            return 0;
        
        return ($pA->_Timestamp < $pB->_Timestamp) ? -1 : 1;
    }

    /**
     * Process all time frames, and their snapshots, 
     *  Checking if its time to create a new snapshot
     *  
     *  @return void
     */
    public function zfsSnapshotCreateNew() {

        // Loop each time frame
        foreach( $this->_Times as $time ) {
            
            $date = strftime( $time->_TimeFormat, time() );
            $now = strptime( $date, $time->_TimeFormat );
            
            if ($time->_Time === $now ) {

                // loop the filesystems and find any that doesnt yet have a snapshot
                foreach( $time->_Filesystems as $fs ) {
                    
                    $timeSnaps = $time->_Snapshots[ $fs->_Name ];
                    
                    $snapCount = count( $timeSnaps );
                    
                    // No snapshots for this fs at all?
                    if( $snapCount === 0) {

                        $this->snapshotMake( $fs, null, $time);
                    
                    } else {

                        // Number of snapshots exceeds limit for this time frame?
                        if( $snapCount <= $time->_Keep ) {

                            // Sort by timestamp
                            uasort( $timeSnaps, array($this, 'snapshotSort'));
                            
                            $latest = $timeSnaps[ $snapCount - 1];
                            
                            $this->snapshotMake( $fs, $latest, $time);
                        }
                    }
                }    //foreach filesystem
            }// time == now
        } //foreach time
    }
    
    /**
     * Process all time frames, and their snapshots, 
     *  removing anything that is expired
     * 
     * @return void
     */
    public function zfsSnapshotRemoveOld() {

        // Loop each time frame
        foreach( $this->_Times as $time ) {

            // Loop each filesystem 
            foreach( $time->_Snapshots as $fsDataset => $snapshots ) {

                $fsCount = count( $snapshots );
                
                // Number of snapshots exceeds limit for this time frame?
                if( $fsCount > $time->_Keep ) {
                    
                    // Sort by timestamp
                    uasort( $snapshots, array($this, 'snapshotSort'));

                    $remove = $fsCount - $time->_Keep;
                
                    // Get the snapshots that should be removed
                    $removeSnaps = array_slice( $snapshots, 0, $remove );
                    
                    $snapshots = array_slice( $snapshots, $remove);
                    
                    $fs = $this->findFilesytemByName( $fsDataset );
                    
                    //
                    foreach( $removeSnaps as $snapshot ) {

                        $this->zfsSnapshotRemove( $snapshot, $fs->_Recursive );
                    }
                }
                
                $time->_Snapshots[ $fsDataset ] = $snapshots;
            }

        }
    }
    
    private function log( $str )
    {
        
        $str = str_replace( ZFS_BINARY . ' ', '', $str );
        $str = str_replace( '2>&1', '', $str );
        
        file_put_contents( LOG_FILE, "$str\n" );
        
    }
    
    /**
     * Execute a zfs command
     * 
     * @param string $pZfsAction zfs command
     * @param array  $pOptions   options to pass
     * 
     * @return int Error code from the exec
     */
    private function zfsExecute( $pZfsAction, $pDebug = false, $pOptions = array() ) {
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
        $execute = implode(' ', $args);
        
        echo "$execute\n";
        
        if( $pDebug !== true )
        {
            exec( $execute, $this->_ZfsOutput, $errorcode);
        
            if( $pZfsAction !== ZFS_SNAP_LIST)
                $this->log( $execute );
        }
        
        return $errorcode;
    }
    
    /**
     * Execute a zpool command
     * 
     * @param string $pZpoolAction zpool command
     * @param array  $pOptions     options to pass
     * 
     * @return int Error code from the exec
     */
    private function zPoolExecute( $pZpoolAction, $pDebug = false, $pOptions = array() ) {
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
        $execute = implode(' ', $args);
        
        echo "$execute\n";
        
        if( $pDebug !== true )
            exec( $execute, $this->_ZpoolOutput, $errorcode);
       
        $this->log( $execute );
        
        return $errorcode;
    }
    
    /**
     * Start a pool scrub
     * 
     * @param cPool $pPool The pool to check
     * 
     * @return void
     */
    private function zfsScrubStart( $pPool ) {
 
        $this->zPoolExecute( ZPOOL_SCRUB, DEBUG, $pPool );
    }
    
    /**
     * Get the status of a pools' scrub
     * 
     * @param cPool $pPool The pool to check
     * 
     * @return bool True if a scrub is being executed
     */
    private function zfsScrubStatus( $pPool ) {
        
        $this->zPoolExecute( ZPOOL_STATUS, DEBUG, $pPool->_Name );
        
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
    
    /**
     * Load a list of snapshots from the zfs snapshot list command
     * 
     * @return bool True on successful load
     */
    private function zfsSnapshotLoad() {

        // Execute zfs
        $this->zfsExecute( ZFS_SNAP_LIST, false);

        // Remove header
        if( strpos( array_shift($this->_ZfsOutput), ZFS_SNAP_OUT ) === false )
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
            $fs = $this->findFilesytemByName( $snapshot->_Dataset );
            if( $fs !== false )
                $fs->_ZfsSnapshots[] = $snapshot;
        }

        return true;
    }
    
    /**
     * Create a ZFS Snapshot
     * 
     * @param cSnapshot $pSnapshot  The snapshot object
     * @param bool      $pRecursive Recursively create snapshots?
     * 
     * @return void
     */
    public function zfsSnapshotCreate( $pSnapshot, $pRecursive ) {
        $Flags = '';
                
        if( $pRecursive === true )
            $Flags .= '-r ';

        $this->zfsExecute( ZFS_SNAP_ADD, DEBUG, "{$Flags} {$pSnapshot->_Dataset}@{$pSnapshot->_Snapshot}" );
    }
    
    /**
     * Destroy a ZFS Snapshot
     * 
     * @param cSnapshot $pSnapshot  The snapshot object
     * @param bool      $pRecursive Recursively create snapshots?
     * 
     * @return void
     */
    private function zfsSnapshotRemove( $pSnapshot, $pRecursive ) {
        $Flags = '';
                
        if( $pRecursive === true )
            $Flags .= '-r ';

        $this->zfsExecute( ZFS_SNAP_REM, DEBUG, "{$Flags} {$pSnapshot->_Dataset}@{$pSnapshot->_Snapshot}" );
    }
    
    /**
     * Process the snapshot name to obtain the date/time of the snapshot
     * 
     * @param cFilesystem $pFilesystem The Filesystem being processed
     * @param cTime       &$pTime       The timeframe being operated in
     * 
     * @return void
     */
    private function ZfsSnapshotProcessTime( $pFilesystem, &$pTime ) {

        foreach( $pFilesystem->_ZfsSnapshots as &$snap ) {
            
            $date = array();
            
            // Does this snapshot match the zfs tag?
            if( ($date = strptime( $snap->_Snapshot, $pTime->_SnapshotFormat )) === false)
                continue;

            // weekly bug (strptime doesnt work with %W, its not implemented in LIBC, atleast on freebsd)
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
        
        unset($snap);
    }
}

/**
 * CLI Entry point
 */
function main($argc, array $argv) { 

    $rev = substr( ZFS_SNAP_REVISION, 11, -2 ) . ' @ ' . substr( ZFS_SNAP_REVISION_DATE, 7, 19 );
    
    $snapver = "zfs-snapshots (svn revision: $rev)\n\n";
    echo $snapver;
    
    $snaps = new cSnapshots( ZFS_SNAP_CONF );
    
    $snaps->zfsSnapshotRemoveOld();
    $snaps->zfsSnapshotCreateNew();
    $snaps->poolProcess();
}
