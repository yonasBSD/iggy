<script lang="ts">
  import { updated } from '$app/state';
  import Button from '$lib/components/Button.svelte';
  import Checkbox from '$lib/components/Checkbox.svelte';
  import Icon from '$lib/components/Icon.svelte';
  import Input from '$lib/components/Input.svelte';
  import PasswordInput from '$lib/components/PasswordInput.svelte';
  import { persistedStore } from '$lib/utils/persistedStore.js';
  import { onMount } from 'svelte';
  import { superForm } from 'sveltekit-superforms/client';

  interface Props {
    data: any;
  }

  let { data }: Props = $props();
  const { form, constraints, errors, message, reset } = superForm(data.form, {});

  const remember = persistedStore('rememberMe', { rememberMe: true, username: '', password: '' });

  onMount(() => {
    if ($remember.rememberMe) {
      $form.username = $remember.username;
      $form.password = $remember.password;
    }
  });
</script>

<form
  method="POST"
  onsubmit={() => {
    if ($remember.rememberMe) {
      $remember.username = $form.username;
      $remember.password = $form.password;
    } else {
      $remember.username = '';
      $remember.password = '';
    }
  }}
  class="min-w-[350px] max-w-[400px] bg-white dark:bg-shadeD700 border text-color p-5 rounded-2xl card-shadow dark:shadow-lg flex flex-col gap-5"
>
  <span class="mx-auto font-semibold">Admin sign in</span>

  <Input
    label="Username"
    name="username"
    errorMessage={$errors?.username?.join(',')}
    bind:value={$form.username}
    {...$constraints.username}
  />

  <PasswordInput
    label="Password"
    name="password"
    errorMessage={$errors.password?.join(',')}
    bind:value={$form.password}
    {...$constraints.password}
  />

  {#if $message}
    <span class="text-sm mx-auto text-red">{$message}</span>
  {/if}

  <div class="flex justify-between items-center">
    <label class="flex gap-1 items-center w-fit hover:cursor-pointer">
      <Checkbox bind:checked={$remember.rememberMe} value="rememberMe" />
      <span class="text-xs font-light">Remember me</span>
    </label>
  </div>

  <Button variant="contained" size="lg" class="mt-7" type="submit">
    <span> Login </span>
    <Icon name="login" class="w-[23px] h-[23px]" />
  </Button>

  <span></span>
</form>
