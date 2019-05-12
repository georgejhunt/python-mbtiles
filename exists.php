<?php
$db = $_GET['db'];

function summary(){
  global $db;
    // Open the database
  try {
    $conn = new PDO("sqlite:$db");
    $sql = 'select zoom_level, min(tile_column),max(tile_column),min(tile_row),max(tile_row), count(zoom_level) from tiles group by zoom_level;';
   $q = $conn->prepare($sql);
   if ( $q ) {
      $q->execute(); 
      $q->bindColumn(1, $zoom);
      $q->bindColumn(2, $xmin);
      $q->bindColumn(3, $xmax);
      $q->bindColumn(4, $ymin);
      $q->bindColumn(5, $ymax);
      $q->bindColumn(6, $count);
      while($q->fetch()) {
         echo $zoom." ".  $xmin." ".  $xmax." ".  $ymin ." ". $ymax ." ". $count;
         echo '<br>' ;
      }
   } else {
      print $sql;
      die("database $db did not open");
   }
  }
  catch(PDOException $e)
  {
    print 'Exception : '.$e->getMessage();
  }
}

function deg2num($lat, $lon, $zoom){ 
   $xtile = floor((($lon + 180) / 360) * pow(2, $zoom));
   $ytile = floor((1 - log(tan(deg2rad($lat)) + 1 / cos(deg2rad($lat))) / pi()) /2 * pow(2, $zoom));
   return array($xtile,$ytile);
}

if (isset($_GET['summary'])){
   summary();
   exit(1);
}
$zoom = $_GET['z'];
$column = $_GET['x'];
$row = $_GET['y'];
  try
  {
    // Open the database
    $conn = new PDO("sqlite:$db");
    // Query the tiles view and echo out the returned image
   $sql = "SELECT * FROM tiles WHERE zoom_level = $zoom AND tile_column = $column AND tile_row = $row";
   $q = $conn->prepare($sql);
   $q->execute();
   $row = $q->fetch(PDO::FETCH_ASSOC);

   if ( $row) {
         header("Content-Type: application/json");
         echo '{"success": "true"}';
      } else {
         header("Content-Type: application/json");
         echo '{"success": "false"}';
      }
  }
  catch(PDOException $e)
  {
         header("Content-Type: application/json");
         echo '{"success": "false"}';
    print 'Exception : '.$e->getMessage();
  }

?>
