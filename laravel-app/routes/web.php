<?php

use App\Http\Controllers\BlockPreviewController;
use Illuminate\Support\Facades\Route;

Route::get('/', function () {
    return view('welcome');
});

Route::middleware(['web', 'auth'])->post('/blocks/preview', BlockPreviewController::class)->name('blocks.preview');
