<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;

class QueryRegistry extends Model
{
    protected $table = 'query_registry';

    protected $primaryKey = 'key';

    public $incrementing = false;

    protected $keyType = 'string';

    public $timestamps = false;

    protected $guarded = [];
}
