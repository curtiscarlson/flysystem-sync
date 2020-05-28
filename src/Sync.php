<?php

namespace TCB\Flysystem;

use League\Flysystem\FilesystemInterface;

/**
 * Class Sync
 *
 * @author Thad Bryson <thadbry@gmail.com>
 */
class Sync
{
    /**
     * Master filesystem.
     *
     * @var FilesystemInterface
     */
    protected $master;

    /**
     * Slave filesystem.
     *
     * @var FilesystemInterface
     */
    protected $slave;

    /**
     * Util object for getting WRITE, UPDATE, and DELETE paths.
     *
     * @var Util
     */
    protected $util;



    /**
     * Sync constructor.
     *
     * @param FilesystemInterface $master
     * @param FilesystemInterface $slave
     * @param string              $dir = '/'
     */
    public function __construct(FilesystemInterface $master, FilesystemInterface $slave, $dir = '/')
    {
        $this->master = $master;
        $this->slave  = $slave;

        $this->util = new Util($master, $slave, $dir);
    }

    /**
     * Get Util helper object used for getting WRITE, UPDATE, and DELETE paths.
     *
     * @return Util
     */
    public function getUtil($path=null)
    {
        return $this->util;
    }

    public function setFolder($path='/')
    {
      $this->util = new Util($this->master, $this->slave, $path);
      return $this;
    }

    public function exclude($path=null)
    {
      if($path) $this->excludes[] = $path;
    }

    /**
     * Call ->put() on $slave. Update/Write content from $master. Also sets visibility on slave.
     *
     * @param $path
     * @return void
     */
    protected function put($path)
    {
        // A dir? Create it.
        if ($path['dir'] === true) {
            $this->slave->createDir($path['path']);
        }
        // Otherwise create or update the file.
        else {
            return $response = $this->slave->putStream($path['path'], $this->master->readStream($path['path']));
        }
    }

    /**
     * Sync any writes.
     *
     * @return $this
     */
    public function syncWrites($writes)
    {
        $requests = 0;
        $start = time();
        foreach ($writes as $path) {
            $now = time();
            $requests++;
            if($now > $start) {
              $start = $now;
              $requests = 1;
            }
            if($requests > 100) { // rate limit to 100 requests per second
              echo PHP_EOL . "sleeping";
              sleep(1);
            }
            $result = $this->put($path);
            if(!$result) {
              echo PHP_EOL . "Error putting: $path";
            }
        }

        return $this;
    }

    /**
     * Sync any deletes.
     *
     * @return $this
     */
    public function syncDeletes($deletes)
    {
        $requests = 0;
        $start = time();

        foreach ($deletes as $path) {

            $now = time();
            $requests++;
            if($now > $start) {
              $start = $now;
              $requests = 1;
            }
            if($requests > 100) { // rate limit to 100 requests per second
              echo PHP_EOL . "sleeping";
              sleep(1);
            }

            // A dir delete may of deleted this path already.
            if ($this->slave->has($path['path']) === false) {
                continue;
            }
            // A dir? They're deleted a special way.
            elseif ($path['dir'] === true) {
                $result = $this->slave->deleteDir($path['path']);
            }
            else {
                $result = $this->slave->delete($path['path']);
            }

            if(!$result) {
              echo PHP_EOL . "Error deleting: $path";
            }
        }

        return $this;
    }

    /**
     * Sync any updates.
     *
     * @return $this
     */
    public function syncUpdates($updates)
    {
        $requests = 0;
        $start = time();
        foreach ($updates as $path) {
            $now = time();
            $requests++;
            if($now > $start) {
              $start = $now;
              $requests = 1;
            }
            if($requests > 100) { // rate limit to 100 requests per second
              echo PHP_EOL . "sleeping";
              sleep(1);
            }
            $result = $this->put($path);
            if(!$result) {
              echo PHP_EOL . "Error putting: $path";
            }
        }

        return $this;
    }

    /**
     * Call $this->syncWrites(), $this->syncUpdates(), and $this->syncDeletes()
     *
     * @return $this
     */
     public function sync($folder = null)
     {
         if($folder) {
           $this->setFolder($folder);
         }

         $writes = $this->util->getWrites();

         echo PHP_EOL . "Syncing " . count($writes) . " writes";

         $deletes = $this->util->getDeletes();

         echo PHP_EOL . "Syncing " . count($deletes) . " deletes";

         $updates = $this->util->getUpdates();

         echo PHP_EOL . "Syncing " . count($updates) . " updates";

         $start = time();

         $this
             ->syncWrites($writes)
             ->syncUpdates($updates)
             ->syncDeletes($deletes)
         ;

         echo PHP_EOL . (time() - $start) . " seconds" . PHP_EOL;

         return $this;
     }
}
