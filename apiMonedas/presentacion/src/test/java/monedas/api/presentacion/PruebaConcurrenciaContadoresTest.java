package monedas.api.presentacion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import monedas.api.aplicacion.concurrencia.ContadorIntentosLogin;
import monedas.api.aplicacion.concurrencia.ContadorIntentosLogin.Variante;
import monedas.api.aplicacion.seguridad.SeguridadServicio;
import monedas.api.aplicacion.servicios.UsuarioServicio;
import monedas.api.core.repositorios.IUsuarioRepositorio;

/**
 * Laboratorio de Control de Concurrencia:
 * Evalúa el comportamiento funcional y de rendimiento de tres variantes de contador
 * instrumentadas en {@link UsuarioServicio#login(String, String)}:
 * <ul>
 *   <li>Variante A: primitivo {@code long} sin sincronización ({@code contador++})</li>
 *   <li>Variante B: exclusión mutua mediante {@code synchronized}</li>
 *   <li>Variante C: operación atómica sin bloqueos ({@code AtomicLong})</li>
 * </ul>
 */
public class PruebaConcurrenciaContadoresTest {

    private static final int HILOS_CONCURRENTES = 32;
    private static final int[] NIVELES_RAFAGA = {500, 1000, 2000, 10000, 100000};

    private record ResultadoVariante(
            Variante variante,
            int tareasEsperadas,
            long valorFinal,
            long inconsistencias,
            int erroresEjecucion,
            double tiempoMs,
            double throughputOpsPorMs
    ) {}

    private UsuarioServicio crearServicioInstrumentado(ContadorIntentosLogin contador) {
        IUsuarioRepositorio repoSimulado = mock(IUsuarioRepositorio.class);
        // Simulamos intentos de login sin latencia de red/BD para maximizar la contención real sobre el contador
        when(repoSimulado.login(anyString(), anyString())).thenReturn(null);
        SeguridadServicio seguridadServicio = new SeguridadServicio();
        return new UsuarioServicio(repoSimulado, seguridadServicio, contador);
    }

    private ResultadoVariante ejecutarRafagaLogin(UsuarioServicio servicio,
                                                  ContadorIntentosLogin contador,
                                                  Variante variante,
                                                  int totalTareas,
                                                  int numHilos) throws InterruptedException {
        contador.reiniciar();
        contador.setVarianteActiva(variante);

        ExecutorService ejecutor = Executors.newFixedThreadPool(numHilos);
        CountDownLatch hilosListos = new CountDownLatch(numHilos);
        CountDownLatch senalInicio = new CountDownLatch(1);
        CountDownLatch finTareas = new CountDownLatch(numHilos);
        AtomicInteger errores = new AtomicInteger(0);

        int tareasBasePorHilo = totalTareas / numHilos;
        int tareasRestantes = totalTareas % numHilos;

        for (int h = 0; h < numHilos; h++) {
            final int tareasHilo = tareasBasePorHilo + (h < tareasRestantes ? 1 : 0);
            final int idHilo = h;
            ejecutor.submit(() -> {
                try {
                    hilosListos.countDown();
                    senalInicio.await();
                    for (int i = 0; i < tareasHilo; i++) {
                        servicio.login("usuario_" + idHilo, "clave_invalida");
                    }
                } catch (Exception e) {
                    errores.incrementAndGet();
                } finally {
                    finTareas.countDown();
                }
            });
        }

        // Esperar a que todos los hilos estén en la línea de salida antes de iniciar el cronómetro
        hilosListos.await();
        long inicioNs = System.nanoTime();
        senalInicio.countDown();
        boolean finalizado = finTareas.await(30, TimeUnit.SECONDS);
        long finNs = System.nanoTime();

        ejecutor.shutdown();
        ejecutor.awaitTermination(10, TimeUnit.SECONDS);

        if (!finalizado) {
            errores.incrementAndGet();
        }

        long valorFinal = contador.obtenerConteo(variante);
        long inconsistencias = Math.abs(totalTareas - valorFinal);
        double tiempoMs = (finNs - inicioNs) / 1_000_000.0;
        double throughput = tiempoMs > 0 ? totalTareas / tiempoMs : 0.0;

        return new ResultadoVariante(
                variante,
                totalTareas,
                valorFinal,
                inconsistencias,
                errores.get(),
                tiempoMs,
                throughput
        );
    }

    private ResultadoVariante ejecutarRafagaContencionPura(ContadorIntentosLogin contador,
                                                           Variante variante,
                                                           int totalTareas,
                                                           int numHilos) throws InterruptedException {
        contador.reiniciar();
        contador.setVarianteActiva(variante);

        ExecutorService ejecutor = Executors.newFixedThreadPool(numHilos);
        CountDownLatch hilosListos = new CountDownLatch(numHilos);
        CountDownLatch senalInicio = new CountDownLatch(1);
        CountDownLatch finTareas = new CountDownLatch(numHilos);
        AtomicInteger errores = new AtomicInteger(0);

        int tareasBasePorHilo = totalTareas / numHilos;
        int tareasRestantes = totalTareas % numHilos;

        for (int h = 0; h < numHilos; h++) {
            final int tareasHilo = tareasBasePorHilo + (h < tareasRestantes ? 1 : 0);
            ejecutor.submit(() -> {
                try {
                    hilosListos.countDown();
                    senalInicio.await();
                    for (int i = 0; i < tareasHilo; i++) {
                        contador.registrarIntento();
                    }
                } catch (Exception e) {
                    errores.incrementAndGet();
                } finally {
                    finTareas.countDown();
                }
            });
        }

        hilosListos.await();
        long inicioNs = System.nanoTime();
        senalInicio.countDown();
        boolean finalizado = finTareas.await(30, TimeUnit.SECONDS);
        long finNs = System.nanoTime();

        ejecutor.shutdown();
        ejecutor.awaitTermination(10, TimeUnit.SECONDS);

        if (!finalizado) {
            errores.incrementAndGet();
        }

        long valorFinal = contador.obtenerConteo(variante);
        long inconsistencias = Math.abs(totalTareas - valorFinal);
        double tiempoMs = (finNs - inicioNs) / 1_000_000.0;
        double throughput = tiempoMs > 0 ? totalTareas / tiempoMs : 0.0;

        return new ResultadoVariante(
                variante,
                totalTareas,
                valorFinal,
                inconsistencias,
                errores.get(),
                tiempoMs,
                throughput
        );
    }

    @Test
    @DisplayName("Comparativa de Concurrencia en UsuarioServicio.login(...): Sin Control vs synchronized vs AtomicLong")
    public void pruebaComparativaContadoresLogin() throws InterruptedException {
        ContadorIntentosLogin contador = new ContadorIntentosLogin();
        UsuarioServicio servicio = crearServicioInstrumentado(contador);

        // Calentamiento JVM (JIT warmup) para estabilizar tiempos de ejecución
        for (Variante v : Variante.values()) {
            ejecutarRafagaLogin(servicio, contador, v, 2000, HILOS_CONCURRENTES);
        }

        System.out.println("======================================================================================================");
        System.out.println(" LABORATORIO DE CONTROL DE CONCURRENCIA - INSTRUMENTACION EN UsuarioServicio.login(...)");
        System.out.println(" Hilos concurrentes por rafaga: " + HILOS_CONCURRENTES);
        System.out.println("======================================================================================================");
        System.out.printf(Locale.US, "%-16s | %10s | %10s | %14s | %8s | %10s | %14s%n",
                "Variante", "Esperado", "Obtenido", "Inconsistencias", "Errores", "Tiempo(ms)", "Ops/ms");
        System.out.println("------------------------------------------------------------------------------------------------------");

        List<ResultadoVariante> resultadosLogin = new ArrayList<>();

        for (int carga : NIVELES_RAFAGA) {
            for (Variante variante : Variante.values()) {
                ResultadoVariante r = ejecutarRafagaLogin(servicio, contador, variante, carga, HILOS_CONCURRENTES);
                resultadosLogin.add(r);
                System.out.printf(Locale.US, "%-16s | %10d | %10d | %14d | %8d | %10.3f | %14.1f%n",
                        etiquetaVariante(r.variante()),
                        r.tareasEsperadas(),
                        r.valorFinal(),
                        r.inconsistencias(),
                        r.erroresEjecucion(),
                        r.tiempoMs(),
                        r.throughputOpsPorMs());

                assertEquals(0, r.erroresEjecucion(), "No deben ocurrir excepciones durante la ejecución");
                if (variante == Variante.SINCRONIZADO || variante == Variante.ATOMICO) {
                    assertEquals(carga, r.valorFinal(),
                            "La variante " + variante + " debe preservar la integridad exacta del contador");
                    assertEquals(0, r.inconsistencias(),
                            "La variante " + variante + " no debe presentar inconsistencias");
                }
            }
            System.out.println("------------------------------------------------------------------------------------------------------");
        }

        // Benchmark adicional de contención pura sobre el contador (1,000,000 incrementos en 32 hilos)
        int cargaExtrema = 1_000_000;
        for (Variante v : Variante.values()) {
            ejecutarRafagaContencionPura(contador, v, 50_000, HILOS_CONCURRENTES);
        }

        System.out.println();
        System.out.println("======================================================================================================");
        System.out.println(" BENCHMARK DE CONTENCION PURA SOBRE EL CONTADOR (" + cargaExtrema + " incrementos / " + HILOS_CONCURRENTES + " hilos)");
        System.out.println("======================================================================================================");
        System.out.printf(Locale.US, "%-16s | %10s | %10s | %14s | %8s | %10s | %14s%n",
                "Variante", "Esperado", "Obtenido", "Inconsistencias", "Errores", "Tiempo(ms)", "Ops/ms");
        System.out.println("------------------------------------------------------------------------------------------------------");

        for (Variante variante : Variante.values()) {
            ResultadoVariante r = ejecutarRafagaContencionPura(contador, variante, cargaExtrema, HILOS_CONCURRENTES);
            System.out.printf(Locale.US, "%-16s | %10d | %10d | %14d | %8d | %10.3f | %14.1f%n",
                    etiquetaVariante(r.variante()),
                    r.tareasEsperadas(),
                    r.valorFinal(),
                    r.inconsistencias(),
                    r.erroresEjecucion(),
                    r.tiempoMs(),
                    r.throughputOpsPorMs());

            assertEquals(0, r.erroresEjecucion(), "No deben ocurrir excepciones en contención pura");
            if (variante == Variante.SINCRONIZADO || variante == Variante.ATOMICO) {
                assertEquals(cargaExtrema, r.valorFinal(),
                        "La variante " + variante + " debe ser 100% consistente bajo contención extrema");
            }
        }
        System.out.println("======================================================================================================");
    }

    private String etiquetaVariante(Variante variante) {
        return switch (variante) {
            case SIN_CONTROL -> "A: Sin control";
            case SINCRONIZADO -> "B: synchronized";
            case ATOMICO -> "C: AtomicLong";
        };
    }
}
