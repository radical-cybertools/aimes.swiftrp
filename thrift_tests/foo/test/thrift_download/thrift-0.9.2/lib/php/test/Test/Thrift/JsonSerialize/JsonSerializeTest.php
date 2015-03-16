<?php
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

namespace Test\Thrift\JsonSerialize;

use stdClass;
use Test\Thrift\Fixtures;
use Thrift\ClassLoader\ThriftClassLoader;
use Thrift\Serializer\TBinarySerializer;

require_once __DIR__.'/../../../../lib/Thrift/ClassLoader/ThriftClassLoader.php';

$loader = new ThriftClassLoader();
$loader->registerNamespace('Thrift', __DIR__ . '/../../../../lib');
$loader->registerNamespace('Test', __DIR__ . '/../../..');
$loader->registerDefinition('ThriftTest', __DIR__ . '/../../../packages/phpjs');
$loader->register();

class JsonSerializeTest extends \PHPUnit_Framework_TestCase
{

  public function testEmptyStruct() {
    $empty = new \ThriftTest\EmptyStruct(array('non_existing_key' => 'bar'));
    $this->assertEquals(new stdClass, json_decode(json_encode($empty)));
  }

  public function testStringsAndInts() {
    $input = array(
      'string_thing' => 'foo',
      'i64_thing' => 1234567890,
    );
    $xtruct = new \ThriftTest\Xtruct($input);

    // Xtruct's 'i32_thing' and 'byte_thing' fields should not be present here!
    $expected = new stdClass;
    $expected->string_thing = $input['string_thing'];
    $expected->i64_thing = $input['i64_thing'];
    $this->assertEquals($expected, json_decode(json_encode($xtruct)));
  }

  public function testNestedStructs() {
    $xtruct2 = new \ThriftTest\Xtruct2(array(
      'byte_thing' => 42,
      'struct_thing' => new \ThriftTest\Xtruct(array(
        'i32_thing' => 123456,
      )),
    ));

    $expected = new stdClass;
    $expected->byte_thing = $xtruct2->byte_thing;
    $expected->struct_thing = new stdClass;
    $expected->struct_thing->i32_thing = $xtruct2->struct_thing->i32_thing;
    $this->assertEquals($expected, json_decode(json_encode($xtruct2)));
  }

  public function testInsanity() {
    $xinput = array('string_thing' => 'foo');
    $xtruct = new \ThriftTest\Xtruct($xinput);
    $insanity = new \ThriftTest\Insanity(array(
      'xtructs' => array($xtruct, $xtruct, $xtruct)
    ));
    $expected = new stdClass;
    $expected->xtructs = array((object)$xinput, (object)$xinput, (object)$xinput);
    $this->assertEquals($expected, json_decode(json_encode($insanity)));
  }

  public function testNestedLists() {
    $bonk = new \ThriftTest\Bonk(array('message' => 'foo'));
    $nested = new \ThriftTest\NestedListsBonk(array('bonk' => array(array(array($bonk)))));
    $expected = new stdClass;
    $expected->bonk = array(array(array((object)array('message' => 'foo'))));
    $this->assertEquals($expected, json_decode(json_encode($nested)));
  }

}
