<?php

namespace App\Http\Controllers;

use App\Exceptions\InvalidBlockConfigurationException;
use App\Models\BlockDefinition;
use App\Models\BlockInstance;
use App\Services\BlockRuntime;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Log;
use Symfony\Component\HttpFoundation\Response;
use Throwable;

class BlockPreviewController extends Controller
{
    public function __invoke(Request $request, BlockRuntime $runtime): JsonResponse
    {
        $validated = $request->validate([
            'definition_id' => ['required', 'integer', 'exists:block_definitions,id'],
            'config' => ['array'],
        ]);

        $definition = BlockDefinition::query()->findOrFail($validated['definition_id']);
        $config = $validated['config'] ?? [];

        $instance = new BlockInstance([
            'block_definition_id' => $definition->id,
            'zone' => 'preview',
            'position' => 0,
            'config' => $config,
            'enabled' => true,
        ]);

        $instance->setRelation('definition', $definition);

        try {
            $runtime->validateConfig($definition, $config);
            $html = $runtime->renderInstance($instance) ?? '';
        } catch (InvalidBlockConfigurationException $exception) {
            return response()->json([
                'ok' => false,
                'errors' => $exception->getErrors(),
            ], Response::HTTP_UNPROCESSABLE_ENTITY);
        } catch (Throwable $throwable) {
            Log::error('Block preview failed to render.', [
                'definition_id' => $definition->id,
                'config' => $config,
                'exception' => $throwable,
            ]);

            return response()->json([
                'ok' => false,
                'errors' => [$throwable->getMessage()],
            ], Response::HTTP_INTERNAL_SERVER_ERROR);
        }

        return response()->json([
            'ok' => true,
            'html' => $html,
        ]);
    }
}
