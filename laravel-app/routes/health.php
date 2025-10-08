<?php

use Illuminate\Support\Facades\Route;

Route::get('/up', function () {
    return ['status' => 'OK'];
});
