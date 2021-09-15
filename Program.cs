using System;
using System.Collections.Generic;
using System.Globalization;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.StreamProcessing.Aggregates;
using System.IO;
using System.Net;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Numerics;
using System.Timers;

using MathNet.Numerics.LinearAlgebra;
using System.Threading;
using System.Threading.Tasks;
using System.Reactive.Subjects;
using System.Linq;
using System.Collections.Concurrent;

namespace ClimateNetwork
{
    internal struct JointInput
    {
        public double X;
        public double Y;
        public string XLoc;
        public string YLoc;

        public override string ToString()
        {
            return $"X: {X}, Y: {Y}, XLoc: {XLoc}, YLoc: {YLoc}\n";
        }
    }

    internal struct StdState
    {
        public string XLoc;
        public string YLoc;

        public int Count;
        public double SumOfX;
        public double SumSquaredOfX;
        public double SumOfY;
        public double SumSquaredOfY;
        public double SumOfXY;
    }

    internal static class StdExtensions
    {
        public static IAggregate<TSource, StdState, double> Std<TKey, TSource>(
            this Window<TKey, TSource> window, Expression<Func<TSource, JointInput>> selector)
        {
            var aggregate = new StdAggregate();
            return aggregate.Wrap(selector);
        }
    }

    internal sealed class StdAggregate : IAggregate<JointInput, StdState, double>
    {
        public Expression<Func<StdState>> InitialState()
        {
            // TODO
            return () => default;
        }

        public Expression<Func<StdState, long, JointInput, StdState>> Accumulate()
        {
            return (oldState, timestamp, input) => new StdState()
            {
                XLoc = input.XLoc,
                YLoc = input.YLoc,
                Count = oldState.Count + 1,
                SumOfX = oldState.SumOfX + input.X,
                SumOfY = oldState.SumOfY + input.Y,
                SumSquaredOfX = oldState.SumSquaredOfX + input.X * input.X,
                SumSquaredOfY = oldState.SumSquaredOfY + input.Y * input.Y,
                SumOfXY = oldState.SumOfXY + input.X * input.Y
            };
        }

        public Expression<Func<StdState, long, JointInput, StdState>> Deaccumulate()
        {
            return (oldState, timestamp, input) => new StdState()
            {
                XLoc = input.XLoc,
                YLoc = input.YLoc,
                Count = oldState.Count - 1,
                SumOfX = oldState.SumOfX - input.X,
                SumOfY = oldState.SumOfY - input.Y,
                SumSquaredOfX = oldState.SumSquaredOfX - input.X * input.X,
                SumSquaredOfY = oldState.SumSquaredOfY - input.Y * input.Y,
                SumOfXY = oldState.SumOfXY - input.X * input.Y
            };
        }

        public Expression<Func<StdState, StdState, StdState>> Difference()
        {
            return (left, right) => new StdState()
            {
                XLoc = (UtilFuncs.StringToDouble(left.XLoc) - UtilFuncs.StringToDouble(right.XLoc)).ToString(),
                YLoc = (UtilFuncs.StringToDouble(left.YLoc) - UtilFuncs.StringToDouble(right.YLoc)).ToString(),
                Count = left.Count - right.Count,
                SumOfX = left.SumOfX - right.SumOfX,
                SumOfY = left.SumOfY - right.SumOfY,
                SumSquaredOfX = left.SumSquaredOfX - right.SumSquaredOfX,
                SumSquaredOfY = left.SumSquaredOfY - right.SumSquaredOfY,
                SumOfXY = left.SumOfXY - right.SumOfXY
            };
        }

        public Expression<Func<StdState, double>> ComputeResult()
        {
            return state =>
            (state.SumOfXY / state.Count - (state.SumOfX * state.SumOfY) / (state.Count * state.Count))
            /
            (
            Math.Sqrt(
                (state.SumSquaredOfX / state.Count) - ((state.SumOfX * state.SumOfX) / (state.Count * state.Count)))
            *
            Math.Sqrt(
                (state.SumSquaredOfY / state.Count) - ((state.SumOfY * state.SumOfY) / (state.Count * state.Count)))
            );
        }
    }

    internal struct BasicWindowDFTState
    {
        public int Granularity;
        public string XLoc;
        public string YLoc;

        public double SumOfX;
        public double SumOfY;
        public double SumOfXSquared;
        public double SumOfYSquared;

        public double SumSquaredOfXRemained;
        public double SumSquaredOfYRemained;
        public List<double> ListOfRemainedX;
        public List<double> ListOfRemainedY;

        public double SumSquaredOfXRemoved;
        public double SumSquaredOfYRemoved;
        public List<double> ListOfRemovedX;
        public List<double> ListOfRemovedY;

        public List<double> ListOfMeanX;
        public List<double> ListOfMeanY;
        public List<double> ListOfSigmaX;
        public List<double> ListOfSigmaY;
        public List<double> ListOfDXY;

        public List<double> ListOfCorr;
        public double SumOfCorr;
        public int BasicWindowCount;

        public double SumOfCorrBW;

        public BasicWindowDFTState(int granularity = 5)
        {
            Granularity = granularity;
            XLoc = null;
            YLoc = null;

            SumOfX = 0;
            SumOfY = 0;
            SumOfXSquared = 0;
            SumOfYSquared = 0;

            SumSquaredOfXRemained = 0;
            SumSquaredOfYRemained = 0;
            ListOfRemainedX = new List<double>();
            ListOfRemainedY = new List<double>();

            SumSquaredOfXRemoved = 0;
            SumSquaredOfYRemoved = 0;
            ListOfRemovedX = new List<double>();
            ListOfRemovedY = new List<double>();

            ListOfMeanX = new List<double>();
            ListOfMeanY = new List<double>();
            ListOfSigmaX = new List<double>();
            ListOfSigmaY = new List<double>();
            ListOfDXY = new List<double>();

            ListOfCorr = new List<double>();
            SumOfCorr = 0;
            BasicWindowCount = 0;
            SumOfCorrBW = 0;
        }

        public BasicWindowDFTState(BasicWindowDFTState oldState, JointInput input, bool toAdd)
        {
            Granularity = oldState.Granularity;
            XLoc = input.XLoc;
            YLoc = input.YLoc;

            SumOfX = oldState.SumOfX;
            SumOfY = oldState.SumOfY;
            SumOfXSquared = oldState.SumOfXSquared;
            SumOfYSquared = oldState.SumOfYSquared;

            SumSquaredOfXRemained = oldState.SumSquaredOfXRemained;
            SumSquaredOfYRemained = oldState.SumSquaredOfYRemained;
            ListOfRemainedX = oldState.ListOfRemainedX;
            ListOfRemainedY = oldState.ListOfRemainedY;

            SumSquaredOfXRemoved = oldState.SumSquaredOfXRemoved;
            SumSquaredOfYRemoved = oldState.SumSquaredOfYRemoved;
            ListOfRemovedX = oldState.ListOfRemovedX;
            ListOfRemovedY = oldState.ListOfRemovedY;

            ListOfCorr = oldState.ListOfCorr;
            SumOfCorr = oldState.SumOfCorr;
            BasicWindowCount = oldState.BasicWindowCount;
            SumOfCorrBW = oldState.SumOfCorrBW;

            ListOfMeanX = oldState.ListOfMeanX;
            ListOfMeanY = oldState.ListOfMeanY;
            ListOfSigmaX = oldState.ListOfSigmaX;
            ListOfSigmaY = oldState.ListOfSigmaY;
            ListOfDXY = oldState.ListOfDXY;

            if (toAdd)
            {
                SumSquaredOfXRemained += input.X * input.X;
                SumSquaredOfYRemained += input.Y * input.Y;
                ListOfRemainedX.Add(input.X);
                ListOfRemainedY.Add(input.Y);

                if (ListOfRemainedX.Count == Granularity)
                {
                    double avgX = ListOfRemainedX.Average();
                    double avgY = ListOfRemainedY.Average();
                    double stdX = Math.Sqrt(SumSquaredOfXRemained - ListOfRemainedX.Count * avgX * avgX);
                    double stdY = Math.Sqrt(SumSquaredOfYRemained - ListOfRemainedY.Count * avgY * avgY);

                    // update sum and sum squared
                    SumOfX += avgX * Granularity;
                    SumOfY += avgY * Granularity;
                    SumOfXSquared += SumSquaredOfXRemained;
                    SumOfYSquared += SumSquaredOfYRemained;

                    // TODO
                    int N = ListOfRemainedX.Count;
                    //int N = 5;
                    int w = ListOfRemainedX.Count;
                    //int N = (int) (w * 0.75);

                    var listX = UtilFuncs.GetDFTResult(stdX, avgX, w, N, ListOfRemainedX);
                    var listY = UtilFuncs.GetDFTResult(stdY, avgY, w, N, ListOfRemainedY);

                    double d = UtilFuncs.GetEuclideanDistance(listX, listY);

                    ListOfMeanX.Add(avgX);
                    ListOfMeanY.Add(avgY);
                    ListOfSigmaX.Add(stdX);
                    ListOfSigmaY.Add(stdY);
                    ListOfDXY.Add(d);

                    var corr = 1 - 0.5 * d * d;
                    if (Double.IsNaN(corr))
                    {
                        string s = "[";
                        foreach (var x in ListOfRemainedX)
                        {
                            s += x.ToString() + ", ";
                        }
                        s += "], [";
                        foreach (var x in ListOfRemainedY)
                        {
                            s += x.ToString() + ", ";
                        }
                        s += "]";
                        throw new Exception(s.ToString());
                    }
                    ListOfCorr.Add(Math.Abs(corr));
                    SumOfCorr += Math.Abs(corr);
                    BasicWindowCount += 1;
                    SumOfCorrBW += Math.Abs(UtilFuncs.GetCorrInBasicWindow(stdX, stdY, avgX, avgY, ListOfRemainedX, ListOfRemainedY));

                    // Reset remained values
                    SumSquaredOfXRemained = 0;
                    SumSquaredOfYRemained = 0;
                    ListOfRemainedX.Clear();
                    ListOfRemainedY.Clear();
                }
            }
            else
            {
                // TODO
                SumSquaredOfXRemoved += input.X * input.X;
                SumSquaredOfYRemoved += input.Y * input.Y;
                ListOfRemovedY.Add(input.X);
                ListOfRemovedY.Add(input.Y);

                if (ListOfRemovedX.Count == Granularity)
                {
                    double avgX = ListOfRemovedX.Average();
                    double avgY = ListOfRemovedY.Average();
                    double stdX = Math.Sqrt(SumSquaredOfXRemoved - ListOfRemovedX.Count * avgX * avgX);
                    double stdY = Math.Sqrt(SumSquaredOfYRemoved - ListOfRemovedY.Count * avgY * avgY);

                    // TODO change number of kept coefficient
                    int N = ListOfRemovedX.Count;
                    int w = ListOfRemovedX.Count;

                    var listX = UtilFuncs.GetDFTResult(stdX, avgX, w, N, ListOfRemovedX);
                    var listY = UtilFuncs.GetDFTResult(stdY, avgY, w, N, ListOfRemovedY);

                    double d = UtilFuncs.GetEuclideanDistance(listX, listY);
                    var corr = 1 - 0.5 * d * d;
                    SumOfCorr -= Math.Abs(corr);
                    BasicWindowCount -= 1;
                    SumOfCorrBW -= Math.Abs(UtilFuncs.GetCorrInBasicWindow(stdX, stdY, avgX, avgY, ListOfRemainedX, ListOfRemainedY));

                    // Reset removed values
                    SumSquaredOfXRemoved = 0;
                    SumSquaredOfYRemoved = 0;
                    ListOfRemovedX.Clear();
                    ListOfRemovedY.Clear();
                }
            }
        }

        public BasicWindowDFTState(BasicWindowDFTState left, BasicWindowDFTState right)
        {
            Granularity = left.Granularity - right.Granularity;
            XLoc = (UtilFuncs.StringToDouble(left.XLoc) - UtilFuncs.StringToDouble(right.XLoc)).ToString();
            YLoc = (UtilFuncs.StringToDouble(left.YLoc) - UtilFuncs.StringToDouble(right.YLoc)).ToString();

            SumOfX = left.SumOfX - right.SumOfX;
            SumOfY = left.SumOfY - right.SumOfY;
            SumOfXSquared = left.SumOfXSquared - right.SumOfXSquared;
            SumOfYSquared = left.SumOfYSquared - right.SumOfYSquared;

            SumSquaredOfXRemained = left.SumSquaredOfXRemained - right.SumSquaredOfXRemained;
            SumSquaredOfYRemained = left.SumSquaredOfYRemained - right.SumSquaredOfYRemained;
            ListOfRemainedX = UtilFuncs.ListDifference(left.ListOfRemainedX, right.ListOfRemainedY);
            ListOfRemainedY = UtilFuncs.ListDifference(left.ListOfRemainedY, right.ListOfRemainedY);

            SumSquaredOfXRemoved = left.SumSquaredOfXRemoved - right.SumSquaredOfXRemoved;
            SumSquaredOfYRemoved = left.SumSquaredOfYRemoved - right.SumSquaredOfYRemoved;
            ListOfRemovedX = UtilFuncs.ListDifference(left.ListOfRemovedX, right.ListOfRemovedX);
            ListOfRemovedY = UtilFuncs.ListDifference(left.ListOfRemovedY, right.ListOfRemovedY);

            ListOfMeanX = UtilFuncs.ListDifference(left.ListOfMeanX, right.ListOfMeanX);
            ListOfMeanY = UtilFuncs.ListDifference(left.ListOfMeanY, right.ListOfMeanY);
            ListOfSigmaX = UtilFuncs.ListDifference(left.ListOfSigmaX, right.ListOfSigmaX);
            ListOfSigmaY = UtilFuncs.ListDifference(left.ListOfSigmaY, right.ListOfSigmaY);
            ListOfDXY = UtilFuncs.ListDifference(left.ListOfDXY, right.ListOfDXY);

            ListOfCorr = UtilFuncs.ListDifference(left.ListOfCorr, right.ListOfCorr);
            SumOfCorr = left.SumOfCorr - right.SumOfCorr;
            BasicWindowCount = left.BasicWindowCount - right.BasicWindowCount;
            SumOfCorrBW = left.SumOfCorrBW - right.SumOfCorrBW;
        }
    }

    internal struct BasicWindowDFTResult
    {
        public List<double> ListOfSigmaX;
        public List<double> ListOfSigmaY;
        public List<double> ListOfDXY;
        public List<double> ListOfMeanX;
        public List<double> ListOfMeanY;

        public double CorrBasicWindowDFT;
        public double CorrBasicWindow;
        public List<double> ListOfCorr;
        public int Granularity;

        public double StdX;
        public double StdY;

        public BasicWindowDFTResult(BasicWindowDFTState state)
        {
            ListOfCorr = state.ListOfCorr;
            CorrBasicWindowDFT = state.SumOfCorr / state.BasicWindowCount;
            CorrBasicWindow = state.SumOfCorrBW / state.BasicWindowCount;
            Granularity = state.Granularity;

            ListOfSigmaX = state.ListOfSigmaX;
            ListOfSigmaY = state.ListOfSigmaY;
            ListOfDXY = state.ListOfDXY;
            ListOfMeanX = state.ListOfMeanX;
            ListOfMeanY = state.ListOfMeanY;

            int n = state.Granularity * state.BasicWindowCount;
            StdX = Math.Sqrt((state.SumOfXSquared / n) - ((state.SumOfX * state.SumOfX) / (n * n)));
            StdY = Math.Sqrt((state.SumOfYSquared / n) - ((state.SumOfY * state.SumOfY) / (n * n)));
        }

        public BasicWindowDFTResult(BasicWindowDFTResult bwr)
        {
            Granularity = bwr.Granularity;
            CorrBasicWindow = bwr.CorrBasicWindow;
            CorrBasicWindowDFT = bwr.CorrBasicWindowDFT;
            ListOfCorr = new List<double>(bwr.ListOfCorr);

            ListOfDXY = new List<double>(bwr.ListOfDXY);
            ListOfSigmaX = new List<double>(bwr.ListOfSigmaX);
            ListOfSigmaY = new List<double>(bwr.ListOfSigmaY);
            ListOfMeanX = new List<double>(bwr.ListOfMeanX);
            ListOfMeanY = new List<double>(bwr.ListOfMeanY);

            StdX = bwr.StdX;
            StdY = bwr.StdY;
        }

        public override string ToString()
        {
            return $"CorrBasicWindowDFT: {CorrBasicWindowDFT}, CorrBasicWindow: {CorrBasicWindow}";
        }
    }

    // Basic Window + Discrete Fourier Transform Extensions
    internal static class BasicWindowDFTExtensions
    {
        public static IAggregate<TSource, BasicWindowDFTState, BasicWindowDFTResult> BasicWindowDFT<TKey, TSource>(
            this Window<TKey, TSource> window, Expression<Func<TSource, JointInput>> selector)
        {
            var aggregate = new BasicWindowDFTAggregate();
            return aggregate.Wrap(selector);
        }
    }

    internal sealed class BasicWindowDFTAggregate : IAggregate<JointInput, BasicWindowDFTState, BasicWindowDFTResult>
    {
        public Expression<Func<BasicWindowDFTState>> InitialState()
        {
            // TODO
            return () => new BasicWindowDFTState(100); // TODO Set Granularity
        }

        public Expression<Func<BasicWindowDFTState, long, JointInput, BasicWindowDFTState>> Accumulate()
        {
            return (oldState, timestamp, input) => new BasicWindowDFTState(oldState, input, true);
        }

        public Expression<Func<BasicWindowDFTState, long, JointInput, BasicWindowDFTState>> Deaccumulate()
        {
            return (oldState, timestamp, input) => new BasicWindowDFTState(oldState, input, false);
        }

        public Expression<Func<BasicWindowDFTState, BasicWindowDFTState, BasicWindowDFTState>> Difference()
        {
            return (left, right) => new BasicWindowDFTState(left, right);
        }

        public Expression<Func<BasicWindowDFTState, BasicWindowDFTResult>> ComputeResult()
        {
            return state => new BasicWindowDFTResult(state);
        }
    }

    internal struct BasicWindowInput
    {
        public double X;
        public double Y;
        public string XLoc;
        public string YLoc;

        public override string ToString()
        {
            return $"X: {X}, Y: {Y}, XLoc: {XLoc}, YLoc: {YLoc}\n";
        }
    }

    internal struct BasicWindowResult
    {
        public List<double> ListOfSigmaX;
        public List<double> ListOfSigmaY;
        public List<double> ListOfCXY;
        public List<double> ListOfMeanX;
        public List<double> ListOfMeanY;
        public double SumX;
        public double SumY;
        public double Corr;

        public double StdX;
        public double StdY;

        public int Granularity;
        public string XLoc;
        public string YLoc;

        public BasicWindowResult(BasicWindowState state)
        {
            Granularity = state.Granularity;
            XLoc = state.XLoc;
            YLoc = state.YLoc;
            ListOfCXY = state.ListOfCXY;
            ListOfSigmaX = state.ListOfSigmaX;
            ListOfSigmaY = state.ListOfSigmaY;
            ListOfMeanX = state.ListOfMeanX;
            ListOfMeanY = state.ListOfMeanY;
            SumX = state.SumOfX;
            SumY = state.SumOfY;
            StdX = Math.Sqrt((state.SumSquaredOfX / state.Count) - ((state.SumOfX * state.SumOfX) / (state.Count * state.Count)));
            StdY = Math.Sqrt((state.SumSquaredOfY / state.Count) - ((state.SumOfY * state.SumOfY) / (state.Count * state.Count)));
            Corr = state.SumOfNumerator / (StdX * StdY);
        }

        public BasicWindowResult(BasicWindowResult bwr)
        {
            Granularity = bwr.Granularity;
            XLoc = bwr.XLoc;
            YLoc = bwr.YLoc;
            ListOfCXY = new List<double>(bwr.ListOfCXY);
            ListOfSigmaX = new List<double>(bwr.ListOfSigmaX);
            ListOfSigmaY = new List<double>(bwr.ListOfSigmaY);
            ListOfMeanX = new List<double>(bwr.ListOfMeanX);
            ListOfMeanY = new List<double>(bwr.ListOfMeanY);
            SumX = bwr.SumX;
            SumY = bwr.SumY;
            StdX = bwr.StdX;
            StdY = bwr.StdY;
            Corr = bwr.Corr;
        }

        public override string ToString()
        {
            string res = new string($"BasicWindowResult: XLoc: {XLoc}, YLoc: {YLoc}, SumX: {SumX}, SumY: {SumY}, Corr: {Corr}\n");
            string listOfCXY = "listOfCXY: [";
            foreach (var cxy in ListOfCXY)
            {
                listOfCXY += cxy.ToString() + ", ";
            }
            listOfCXY += "]";
            return res + listOfCXY;
        }
    }

    internal struct BasicWindowState
    {
        public int Granularity;
        public string XLoc;
        public string YLoc;

        public ulong Count;
        public double SumOfX;
        public double SumOfY;
        public double SumSquaredOfX;
        public double SumSquaredOfY;

        public int CountOfRemained;
        public double SumOfXRemained;
        public double SumOfYRemained;
        public double SumSquaredOfXRemained;
        public double SumSquaredOfYRemained;
        public double SumOfXYRemained;

        public double SumOfNumerator;

        public List<double> ListOfMeanX;
        public List<double> ListOfMeanY;
        public List<double> ListOfSigmaX;
        public List<double> ListOfSigmaY;
        public List<double> ListOfCXY;
        // TODO

        public BasicWindowState(int granularity = 5)
        {
            Granularity = granularity;
            // TODO
            XLoc = null;
            YLoc = null;

            Count = 0;
            SumOfX = 0;
            SumOfY = 0;
            SumSquaredOfX = 0;
            SumSquaredOfY = 0;

            CountOfRemained = 0;
            SumOfXRemained = 0;
            SumOfYRemained = 0;
            SumSquaredOfXRemained = 0;
            SumSquaredOfYRemained = 0;
            SumOfXYRemained = 0;

            SumOfNumerator = 0;

            ListOfMeanX = new List<double>();
            ListOfMeanY = new List<double>();
            ListOfSigmaX = new List<double>();
            ListOfSigmaY = new List<double>();
            ListOfCXY = new List<double>();
        }

        public BasicWindowState(BasicWindowState oldState, BasicWindowInput input, bool toAdd)
        {
            Granularity = oldState.Granularity;
            XLoc = input.XLoc;
            YLoc = input.YLoc;

            if (toAdd)
            {
                Count = oldState.Count + 1;
                SumOfX = oldState.SumOfX + input.X;
                SumOfY = oldState.SumOfY + input.Y;
                SumSquaredOfX = oldState.SumSquaredOfX + input.X * input.X;
                SumSquaredOfY = oldState.SumSquaredOfY + input.Y * input.Y;

                CountOfRemained = oldState.CountOfRemained + 1;
                SumOfXRemained = oldState.SumOfXRemained + input.X;
                SumOfYRemained = oldState.SumOfYRemained + input.Y;
                SumSquaredOfXRemained = oldState.SumSquaredOfXRemained + input.X * input.X;
                SumSquaredOfYRemained = oldState.SumSquaredOfYRemained + input.Y * input.Y;
                SumOfXYRemained = oldState.SumOfXYRemained + input.X * input.Y;

                if (CountOfRemained == Granularity)
                {
                    double sigmaX = Math.Sqrt((SumSquaredOfXRemained / CountOfRemained) - (SumOfXRemained * SumOfXRemained) / (CountOfRemained * CountOfRemained));
                    double sigmaY = Math.Sqrt((SumSquaredOfYRemained / CountOfRemained) - (SumOfYRemained * SumOfYRemained) / (CountOfRemained * CountOfRemained));
                    /*double cXY = (CountOfRemained * SumOfXYRemained - SumOfXRemained * SumOfYRemained) /
                        (Math.Sqrt(CountOfRemained * SumSquaredOfXRemained - SumOfXRemained * SumOfXRemained) *
                        Math.Sqrt(CountOfRemained * SumSquaredOfYRemained - SumOfYRemained * SumOfYRemained));*/
                    double cXY = 0.0;
                    cXY = (CountOfRemained * SumOfXYRemained - SumOfXRemained * SumOfYRemained) /
                        (Math.Sqrt(CountOfRemained * SumSquaredOfXRemained - SumOfXRemained * SumOfXRemained) *
                        Math.Sqrt(CountOfRemained * SumSquaredOfYRemained - SumOfYRemained * SumOfYRemained));

                    if ((CountOfRemained * SumOfXYRemained - SumOfXRemained * SumOfYRemained) == 0)
                    {
                        cXY = 0;
                    }
                    if (double.IsNaN(cXY))
                    {
                        throw new ArgumentException($"denominator: {Math.Sqrt(CountOfRemained * SumSquaredOfXRemained - SumOfXRemained * SumOfXRemained) * Math.Sqrt(CountOfRemained * SumSquaredOfYRemained - SumOfYRemained * SumOfYRemained)}" +
                            $" numerator: {(CountOfRemained * SumOfXYRemained - SumOfXRemained * SumOfYRemained)} " +
                            $"SumOfXYRemained: {SumOfXYRemained} SumOfXRemained: {SumOfXRemained} SumOfYRemained: {SumOfYRemained} " +
                            $"SumSquaredOfXRemained: {SumSquaredOfXRemained} SumSquaredOfYRemained: {SumSquaredOfYRemained} " +
                            $"CountOfRemained: {CountOfRemained} sigmaX: {sigmaX} sigmaY: {sigmaY}");
                    }
                    if (double.IsInfinity(cXY))
                    {
                        throw new ArgumentException($"denominator: {Math.Sqrt(CountOfRemained * SumSquaredOfXRemained - SumOfXRemained * SumOfXRemained) * Math.Sqrt(CountOfRemained * SumSquaredOfYRemained - SumOfYRemained * SumOfYRemained)}" +
                            $" numerator: {(CountOfRemained * SumOfXYRemained - SumOfXRemained * SumOfYRemained)} " +
                            $"SumOfXYRemained: {SumOfXYRemained} SumOfXRemained: {SumOfXRemained} SumOfYRemained: {SumOfYRemained} " +
                            $"SumSquaredOfXRemained: {SumSquaredOfXRemained} SumSquaredOfYRemained: {SumSquaredOfYRemained} " +
                            $"CountOfRemained: {CountOfRemained} sigmaX: {sigmaX} sigmaY: {sigmaY}");
                        //cXY = CountOfRemained * SumOfXYRemained - SumOfXRemained * SumOfYRemained > 0 ? 1 : -1;

                    }
                    // Update SumOfNumerator
                    SumOfNumerator = oldState.SumOfNumerator + sigmaX * sigmaY * cXY;

                    ListOfMeanX = oldState.ListOfMeanX;
                    ListOfMeanX.Add(SumOfXRemained / CountOfRemained);
                    ListOfMeanY = oldState.ListOfMeanY;
                    ListOfMeanY.Add(SumOfYRemained / CountOfRemained);
                    ListOfSigmaX = oldState.ListOfSigmaX;
                    ListOfSigmaX.Add(sigmaX);
                    ListOfSigmaY = oldState.ListOfSigmaY;
                    ListOfSigmaY.Add(sigmaY);
                    ListOfCXY = oldState.ListOfCXY;
                    ListOfCXY.Add(cXY);

                    // Reset remained values
                    CountOfRemained = 0;
                    SumOfXRemained = 0;
                    SumOfYRemained = 0;
                    SumSquaredOfXRemained = 0;
                    SumSquaredOfYRemained = 0;
                    SumOfXYRemained = 0;
                }
                else
                {
                    SumOfNumerator = oldState.SumOfNumerator;
                    ListOfMeanX = oldState.ListOfMeanX;
                    ListOfMeanY = oldState.ListOfMeanY;
                    ListOfSigmaX = oldState.ListOfSigmaX;
                    ListOfSigmaY = oldState.ListOfSigmaY;
                    ListOfCXY = oldState.ListOfCXY;
                }
            }
            else // TODO
            {
                Count = oldState.Count - 1;

                SumOfX = oldState.SumOfX - input.X;
                SumOfY = oldState.SumOfY - input.Y;
                SumSquaredOfX = oldState.SumSquaredOfX - input.X * input.X;
                SumSquaredOfY = oldState.SumSquaredOfY - input.Y * input.Y;

                if (oldState.CountOfRemained == 0)
                {
                    CountOfRemained = Granularity - 1;

                    ListOfMeanX = oldState.ListOfMeanX;
                    double lastListOfMeanX = ListOfMeanX[ListOfMeanX.Count - 1];
                    SumOfXRemained = lastListOfMeanX * Granularity - input.X;
                    ListOfMeanY = oldState.ListOfMeanY;
                    double lastListOfMeanY = ListOfMeanY[ListOfMeanY.Count - 1];
                    SumOfYRemained = lastListOfMeanY * Granularity - input.Y;

                    ListOfSigmaX = oldState.ListOfSigmaX;
                    ListOfSigmaY = oldState.ListOfSigmaY;
                    ListOfCXY = oldState.ListOfCXY;

                    double lastListOfSigmaX = ListOfSigmaX[ListOfSigmaX.Count - 1];
                    double lastListOfSigmaY = ListOfSigmaY[ListOfSigmaY.Count - 1];
                    double lastListOfCXY = ListOfCXY[ListOfCXY.Count - 1];
                    double _SumSquaredOfX = Granularity * (lastListOfSigmaX * lastListOfSigmaX + lastListOfMeanX + lastListOfMeanX);
                    double _SumSquaredOfY = Granularity * (lastListOfSigmaY * lastListOfSigmaY + lastListOfMeanY + lastListOfMeanY);
                    SumSquaredOfXRemained = _SumSquaredOfX - input.X * input.X;
                    SumSquaredOfYRemained = _SumSquaredOfY - input.Y * input.Y;
                    double _SumOfXY = (1 / Granularity) *
                        (lastListOfCXY * Math.Sqrt(Granularity * _SumSquaredOfX - lastListOfMeanX * lastListOfMeanX) *
                        Math.Sqrt(Granularity * _SumSquaredOfY - lastListOfMeanY * lastListOfMeanY) +
                        lastListOfMeanX * lastListOfMeanY * Granularity * Granularity);
                    SumOfXYRemained = _SumOfXY - input.X * input.Y;
                    SumOfNumerator = oldState.SumOfNumerator - lastListOfSigmaX * lastListOfSigmaY * _SumOfXY;

                    ListOfMeanX.RemoveAt(ListOfMeanX.Count - 1);
                    ListOfMeanY.RemoveAt(ListOfMeanY.Count - 1);
                    ListOfSigmaX.RemoveAt(ListOfSigmaX.Count - 1);
                    ListOfSigmaY.RemoveAt(ListOfSigmaY.Count - 1);
                    ListOfCXY.RemoveAt(ListOfCXY.Count - 1);
                }
                else
                {
                    CountOfRemained = oldState.CountOfRemained - 1;
                    SumOfXRemained = oldState.SumOfXRemained - input.X;
                    SumOfYRemained = oldState.SumOfYRemained - input.Y;
                    SumSquaredOfXRemained = oldState.SumSquaredOfXRemained - input.X * input.X;
                    SumSquaredOfYRemained = oldState.SumSquaredOfYRemained - input.Y * input.Y;
                    SumOfXYRemained = oldState.SumOfXYRemained - input.X * input.Y;
                    SumOfNumerator = oldState.SumOfNumerator;

                    ListOfMeanX = oldState.ListOfMeanX;
                    ListOfMeanY = oldState.ListOfMeanY;
                    ListOfSigmaX = oldState.ListOfSigmaX;
                    ListOfSigmaY = oldState.ListOfSigmaY;
                    ListOfCXY = oldState.ListOfCXY;
                }
            }
        }

        public BasicWindowState(BasicWindowState left, BasicWindowState right)
        {
            Granularity = left.Granularity - right.Granularity;

            XLoc = (UtilFuncs.StringToDouble(left.XLoc) - UtilFuncs.StringToDouble(right.XLoc)).ToString();
            YLoc = (UtilFuncs.StringToDouble(left.YLoc) - UtilFuncs.StringToDouble(right.YLoc)).ToString();

            Count = left.Count - right.Count;
            SumOfX = left.SumOfX - right.SumOfX;
            SumOfY = left.SumOfY - right.SumOfY;
            SumSquaredOfX = left.SumSquaredOfX - right.SumSquaredOfX;
            SumSquaredOfY = left.SumSquaredOfY - right.SumSquaredOfY;

            CountOfRemained = left.CountOfRemained - right.CountOfRemained;
            SumOfXRemained = left.SumOfXRemained - right.SumOfXRemained;
            SumOfYRemained = left.SumOfYRemained - right.SumOfYRemained;
            SumSquaredOfXRemained = left.SumSquaredOfXRemained - right.SumSquaredOfXRemained;
            SumSquaredOfYRemained = left.SumSquaredOfYRemained - right.SumSquaredOfYRemained;
            SumOfXYRemained = left.SumOfXYRemained - right.SumOfXYRemained;

            SumOfNumerator = left.SumOfNumerator - right.SumOfNumerator;

            ListOfMeanX = UtilFuncs.ListDifference(left.ListOfMeanX, right.ListOfMeanX);
            ListOfMeanY = UtilFuncs.ListDifference(left.ListOfMeanY, right.ListOfMeanY);
            ListOfSigmaX = UtilFuncs.ListDifference(left.ListOfSigmaX, right.ListOfSigmaX);
            ListOfSigmaY = UtilFuncs.ListDifference(left.ListOfSigmaY, right.ListOfSigmaY);
            ListOfCXY = UtilFuncs.ListDifference(left.ListOfCXY, right.ListOfCXY);
        }
    }

    internal static class BasicWindowExtensions
    {
        public static IAggregate<TSource, BasicWindowState, BasicWindowResult> BasicWindow<TKey, TSource>(
            this Window<TKey, TSource> window, Expression<Func<TSource, BasicWindowInput>> selector)
        {
            var aggregate = new BasicWindow();
            return aggregate.Wrap(selector);
        }
    }

    internal sealed class BasicWindow : IAggregate<BasicWindowInput, BasicWindowState, BasicWindowResult>
    {
        public Expression<Func<BasicWindowState>> InitialState()
        {
            // TODO
            return () => new BasicWindowState(10); // TODO Set Granularity
        }

        public Expression<Func<BasicWindowState, long, BasicWindowInput, BasicWindowState>> Accumulate()
        {
            return (oldState, timestamp, input) => new BasicWindowState(oldState, input, true);

        }

        public Expression<Func<BasicWindowState, long, BasicWindowInput, BasicWindowState>> Deaccumulate()
        {
            return (oldState, timestamp, input) => new BasicWindowState(oldState, input, false);
        }

        public Expression<Func<BasicWindowState, BasicWindowState, BasicWindowState>> Difference()
        {
            return (left, right) => new BasicWindowState(left, right);
        }

        public Expression<Func<BasicWindowState, BasicWindowResult>> ComputeResult()
        {
            return state => new BasicWindowResult(state);
        }
    }

    internal struct Point
    {
        public string Date { get; set; }
        public string Time { get; set; }
        public string Longitude { get; set; }
        public string Latitude { get; set; }
        public double Avg_temp { get; set; }
        public long Timestamp { get; set; }
        public DateTime StartTime { get; set; }

        public string Location;
        // TODO new variables can be added

        public Point(string date, string time, string longitude, string latitude, string avg_temp)
        {
            Date = date;
            Time = time;
            Longitude = longitude;
            Latitude = latitude;

            Avg_temp = UtilFuncs.StringToDouble(avg_temp.Trim());

            DateTime dt = DateTime.ParseExact(date + time, "yyyyMMddHHmm", CultureInfo.CurrentCulture);
            Timestamp = UtilFuncs.ConvertToTimestamp(dt);
            //StartTime = dt;
            StartTime = DateTime.Now;

            Location = Longitude + Latitude;
        }

        public Point(Point p) : this(p.Date, p.Time, p.Longitude, p.Latitude, p.Avg_temp.ToString())
        {

        }

        public override string ToString() => $" {{date:{Date}, time:{Time}, longitude:{Longitude}, " +
            $"latitude:{Latitude}, avg_temp:{Avg_temp}, timestamp:{Timestamp}, start time:{StartTime}, " +
            $"location:{Location}}}";
    };

    internal class UtilFuncs    
    {
        private static readonly DateTime origin = new DateTime(2020, 1, 1, 0, 0, 0); // TODO origin time for hourly data

        public static List<double> ListDifference(List<double> left, List<double> right)
        {
            var list1 = left.Where(i => !right.Contains(i)).ToList();
            var list2 = right.Where(i => !left.Contains(i)).ToList();
            list1.AddRange(list2);
            return new List<double>(list1);
        }

        public static Point ConvertToPoint(string s)
        {
            Point point = new Point(s.Substring(6, 8), s.Substring(15, 4), s.Substring(41, 7),
                s.Substring(49, 7), s.Substring(65, 7));
            return point;
        }

        public static double StringToDouble(string s)
        {
            double num;
            if (!Double.TryParse(s.Trim(), out num))
            {
                throw new Exception($"String {s} cannot convert to double.");
            }
            return num;
        }

        public static string GetLoc(Point point)
        {
            return point.Longitude + point.Latitude;
        }

        public static long ConvertToTimestamp(DateTime dt)
        {
            TimeSpan diff = dt - origin;
            return Convert.ToInt64(Math.Floor(diff.TotalHours)); // TODO hourly timestamps
        }

        public static double CalculatePeaesonCoefficient(double avgX, double avgY, double avgXY, double stdX, double stdY)
        {
            double res = (avgXY - avgX * avgY) / (stdX * stdY);
            return Math.Abs(res);
        }

        public static double GetEuclideanDistance(List<Complex> left, List<Complex> right)
        {
            if (left.Count < 1 || right.Count < 1 || left.Count != right.Count)
            {
                throw new ArgumentException("Invalid input for computing Euclidean distance !");
            }
            double res = 0.0;
            for (int i = 0; i < left.Count; i += 1)
            {
                var diff = Complex.Abs(left[i] - right[i]);
                res += diff * diff;
            }
            return Math.Sqrt(res);
        }

        public static List<Complex> GetDFTResult(
            double std, double avg, int w, int N, List<double> xs)
        {
            List<Complex> list = new List<Complex>();
            for (int f = 0; f < N; f += 1)
            {
                Complex sum = 0;
                for (int i = 0; i < w; i += 1)
                {
                    var xi = (xs[i] - avg) / std; // Normalization
                    sum += Complex.FromPolarCoordinates(xi, 2 * Math.PI * f * i / w);
                }
                var Xf = (1 / Math.Sqrt(w)) * sum;
                list.Add(Xf);
            }
            return list;
        }

        public static double GetCorrInBasicWindow(
            double stdX, double stdY, double avgX, double avgY, List<double> xs, List<double> ys)
        {
            double sumX = 0;
            double sumSquaredX = 0;
            double sumY = 0;
            double sumSquaredY = 0;
            double sumXY = 0;
            int n = xs.Count;
            for (int i = 0; i < n; i += 1)
            {
                var xi = (xs[i] - avgX) / stdX;
                var yi = (ys[i] - avgY) / stdY;
                sumX += xi;
                sumSquaredX += xi * xi;
                sumY += yi;
                sumSquaredY += yi * yi;
                sumXY += xi * yi;
            }
            double sX = Math.Sqrt(
                (sumSquaredX / n) - ((sumX * sumX) / (n * n)));
            double sY = Math.Sqrt(
                (sumSquaredY / n) - ((sumY * sumY) / (n * n)));
            double res = ((sumXY / n) - (sumX / n) * (sumY / n)) / (sX * sY);
            //Console.WriteLine(res);
            return res;
        }

        public static double MatrixSimilarity(Matrix<double> left, Matrix<double> right)
        {
            double ratio = 0;
            int n = left.RowCount;
            double count = 0;
            for (int i = 0; i < n; i += 1)
            {
                for (int j = i + 1; j < n; j += 1)
                {
                    if (left[i, j] == right[i, j])
                    {
                        count += 1;
                    }
                }
            }
            ratio = count / (double)(n * (n - 1) / 2);
            return ratio;
        }
    }

    internal class NetworkConstruction
    {
        public static Matrix<double> ConstructNetworkFromMap(
            Dictionary<string, List<Point>> map, double threshold = 0.75)
        {
            List<string> locations = new List<string>();
            Dictionary<string, IObservableIngressStreamable<Point>> mapOfStreams =
                new Dictionary<string, IObservableIngressStreamable<Point>>();
            foreach (var pair in map)
            {
                string location = pair.Key;
                locations.Add(location);
                if (mapOfStreams.ContainsKey(location))
                {
                    throw new ArgumentException($"Duplicated Location: {{{location}}} !");
                }
                List<Point> listOfPoints = pair.Value;
                IObservable<Point> pointObservale = listOfPoints.ToObservable();
                IObservable<StreamEvent<Point>> streamEventObservable =
                    pointObservale.Select(e => StreamEvent.CreateInterval(e.Timestamp, e.Timestamp + 1, e));
                IObservableIngressStreamable<Point> ingressOberservable =
                    streamEventObservable.ToStreamable(DisorderPolicy.Drop());
                mapOfStreams.Add(location, ingressOberservable);
                //ingressOberservable.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();
            }
            Console.WriteLine(mapOfStreams.Count);

            var mb = Matrix<double>.Build;
            var adjacencyMatrix = mb.Dense(map.Count, map.Count, 0);

            ConcurrentDictionary<string, double> resultMap = new ConcurrentDictionary<string, double>();

            Stopwatch sw = new Stopwatch();
            sw.Start();
            int sumOfConnectedPairs = 0;
            int numOfLocations = adjacencyMatrix.RowCount;
            Parallel.For(0, numOfLocations, i =>
            {
                for (int j = i + 1; j < numOfLocations; j += 1)
                {
                    string leftLoc = locations[i];
                    string rightLoc = locations[j];
                    IObservableIngressStreamable<Point> leftIngressOberservable = mapOfStreams[leftLoc];
                    IObservableIngressStreamable<Point> rightIngressOberservable = mapOfStreams[rightLoc];
                    // Cross join
                    var jointOutput = leftIngressOberservable.Join(
                        rightIngressOberservable,
                        (left, right) =>
                        new JointInput { X = left.Avg_temp, Y = right.Avg_temp, XLoc = left.Location, YLoc = right.Location }
                        );

                    // TODO length difference between left and right?
                    var leftLengthOfSeries = map[leftLoc].Count;
                    var rightLengthOfSeries = map[rightLoc].Count;
                    var leftDuration = map[leftLoc][leftLengthOfSeries - 1].Timestamp;
                    var rightDuration = map[rightLoc][rightLengthOfSeries - 1].Timestamp;
                    var duration = Math.Max(leftDuration, rightDuration);
                    var offset = 0;
                    var avgOutput = jointOutput.TumblingWindowLifetime(duration, offset).Aggregate(
                        w => StdExtensions.Std(w, v => v));

                    avgOutput.ToStreamEventObservable().ForEachAsync(e =>
                    {
                        if (e.IsData)
                        {
                            resultMap.TryAdd(Convert.ToString(i * 1000 + j), e.Payload); // TODO
                            //Console.WriteLine(e.Payload);
                            //pearsonCoefficent = e.Payload;
                        }
                    }).Wait();
                }
            });

            foreach (var pair in resultMap)
            {
                double pearsonCoefficent = pair.Value;

                if (Math.Abs(pearsonCoefficent) < threshold)
                {
                    continue;
                }
                int key = Int32.Parse(pair.Key);
                int m = key / 1000;
                int j = key % 1000;
                adjacencyMatrix[m, j] = 1;
                adjacencyMatrix[j, m] = 1;
                sumOfConnectedPairs += 1;
            }

            sw.Stop();
            Console.WriteLine($"Number of connected pairs: {sumOfConnectedPairs}");
            Console.WriteLine($"Number of total pairs: {(numOfLocations * (numOfLocations - 1)) / 2}");
            Console.WriteLine($"Time used for joining: {sw.Elapsed.ToString()}");

            return adjacencyMatrix;
        }

        public static Matrix<double> ConstructNetworkFromMapBasicWindowDFT(
            Dictionary<string, List<Point>> map, ref Dictionary<string, BasicWindowDFTResult> resultMapBWDFT,
            double threshold = 0.75, int size = -1, bool isTestQueryTime = false, bool isStatStream = false, bool isRealtime = false)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            // Store locations in list
            List<string> locations = new List<string>();

            // Convert to streams
            Dictionary<string, IObservableIngressStreamable<Point>> mapOfStreams =
                new Dictionary<string, IObservableIngressStreamable<Point>>();

            foreach (var pair in map)
            {
                string location = pair.Key;
                locations.Add(location);
                if (mapOfStreams.ContainsKey(location))
                {
                    throw new ArgumentException($"Duplicated Location: {{{location}}} !");
                }
                List<Point> listOfPoints = pair.Value;
                IObservable<Point> pointObservale = listOfPoints.ToObservable();
                IObservable<StreamEvent<Point>> streamEventObservable =
                    pointObservale.Select(e => StreamEvent.CreateInterval(e.Timestamp, e.Timestamp + 1, e));
                IObservableIngressStreamable<Point> ingressOberservable =
                    streamEventObservable.ToStreamable(DisorderPolicy.Drop());
                mapOfStreams.Add(location, ingressOberservable);
                //ingressOberservable.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();
            }
            Console.WriteLine(mapOfStreams.Count);

            var mb = Matrix<double>.Build;
            var adjacencyMatrix = mb.Dense(map.Count, map.Count, 0);

            var mbBW = Matrix<double>.Build;
            var adjacencyMatrixBW = mbBW.Dense(map.Count, map.Count, 0);

            ConcurrentDictionary<string, BasicWindowDFTResult> resultMap = new ConcurrentDictionary<string, BasicWindowDFTResult>();

            int sumOfConnectedPairs = 0;
            int sumOfConnectedPairsBW = 0;
            int numOfLocations = adjacencyMatrix.RowCount;
            Parallel.For(0, numOfLocations, i =>
            {
                for (int j = i + 1; j < numOfLocations; j += 1)
                {
                    string leftLoc = locations[i];
                    string rightLoc = locations[j];
                    IObservableIngressStreamable<Point> leftIngressOberservable = mapOfStreams[leftLoc];
                    IObservableIngressStreamable<Point> rightIngressOberservable = mapOfStreams[rightLoc];
                    // Cross join
                    var jointOutput = leftIngressOberservable.Join(
                        rightIngressOberservable,
                        (left, right) =>
                        new JointInput { X = left.Avg_temp, Y = right.Avg_temp, XLoc = left.Location, YLoc = right.Location }
                        );

                    // TODO length difference between left and right?
                    var leftLengthOfSeries = map[leftLoc].Count;
                    var rightLengthOfSeries = map[rightLoc].Count;
                    var leftDuration = map[leftLoc][leftLengthOfSeries - 1].Timestamp;
                    var rightDuration = map[rightLoc][rightLengthOfSeries - 1].Timestamp;
                    var duration = Math.Max(leftDuration, rightDuration);
                    var offset = 0;
                    var avgOutput = jointOutput.TumblingWindowLifetime(duration, offset).Aggregate(
                        w => BasicWindowDFTExtensions.BasicWindowDFT(w, v => v));

                    avgOutput.ToStreamEventObservable().ForEachAsync(e =>
                    {
                        if (e.IsData)
                        {
                            resultMap.TryAdd(Convert.ToString(i * 1000 + j), e.Payload); // TODO
                            //Console.WriteLine(e.Payload);
                            //pearsonCoefficent = e.Payload.CorrBasicWindow;
                            //sumOfConnectedPairsBW = e.Payload.CorrBasicWindowDFT;
                        }
                    }).Wait();
                }
            });
            sw.Stop();
            Console.WriteLine($"Time used for joining: {sw.Elapsed.ToString()}");

            if (isRealtime)
            {
                foreach (var pair in resultMap)
                {
                    resultMapBWDFT.Add(pair.Key, new BasicWindowDFTResult(pair.Value));
                }
                return adjacencyMatrix;
            }


            if (isTestQueryTime && !isStatStream)
            {
                List<int> sizes = new List<int>() { 500, 1000, 1500, 2000, 2500, 3000, 3500, 4000 };
                foreach (int k in sizes)
                {
                    size = k;
                    Console.WriteLine($"query size is : {size}");
                    Stopwatch sw_1 = new Stopwatch();
                    sw_1.Start();
                    int minNum_ = int.MaxValue;
                    foreach (var pair in resultMap)
                    {
                        BasicWindowDFTResult bwdr = pair.Value;
                        int windowNums = size > 0 ? size / bwdr.Granularity : bwdr.ListOfCorr.Count;

                        if (bwdr.ListOfCorr.Count < minNum_)
                        {
                            minNum_ = bwdr.ListOfCorr.Count;
                        }

                        double corr = 0.0;
                        double numerator = 0.0;
                        double demoninator1 = 0.0;
                        double demoninator2 = 0.0;
                        List<double> listOfMeanX = bwdr.ListOfMeanX.GetRange(0, windowNums);
                        List<double> listOfMeanY = bwdr.ListOfMeanY.GetRange(0, windowNums);
                        double meanX = listOfMeanX.Average();
                        double meanY = listOfMeanY.Average();
                        List<double> listOfDeltaX = new List<double>();
                        List<double> listOfDeltaY = new List<double>();
                        foreach (var x in listOfMeanX)
                        {
                            listOfDeltaX.Add(x - meanX);
                        }
                        foreach (var y in listOfMeanY)
                        {
                            listOfDeltaY.Add(y - meanY);
                        }
                        for (int i = 0; i < windowNums; i += 1)
                        {
                            numerator += bwdr.ListOfSigmaX[i] * bwdr.ListOfSigmaY[i] * bwdr.ListOfDXY[i] * bwdr.ListOfDXY[i]
                                - 2 * bwdr.ListOfSigmaX[i] * bwdr.ListOfSigmaY[i] - 2 * listOfDeltaX[i] * listOfDeltaY[i];
                            if (double.IsNaN(numerator))
                            {
                                throw new ArgumentException($"sigma x: {bwdr.ListOfSigmaX[i]} sigma y: {bwdr.ListOfSigmaY[i]} " +
                                    $"dxy: {bwdr.ListOfDXY[i]} delta x: {listOfDeltaX[i]} delta y: {listOfDeltaY[i]}");
                            }
                            demoninator1 += bwdr.ListOfSigmaX[i] * bwdr.ListOfSigmaX[i] + listOfDeltaX[i] * listOfDeltaX[i];
                            demoninator2 += bwdr.ListOfSigmaY[i] * bwdr.ListOfSigmaY[i] + listOfDeltaY[i] * listOfDeltaY[i];
                        }
                        double dSquare = 2 + numerator / (Math.Sqrt(demoninator1) * Math.Sqrt(demoninator2));
                        corr = 1 - 0.5 * dSquare;

                        if (Math.Abs(corr) < threshold)
                        {
                            continue;
                        }
                        int key = Int32.Parse(pair.Key);
                        int m = key / 1000;
                        int j = key % 1000;
                        adjacencyMatrix[m, j] = 1;
                        adjacencyMatrix[j, m] = 1;
                        sumOfConnectedPairs += 1;
                    }
                    Console.WriteLine($"minNum: {minNum_}");
                    sw_1.Stop();
                    Console.WriteLine($"Time used for query: {sw_1.Elapsed.ToString()}");

                    Console.WriteLine($"Number of connected pairs in DFT: {sumOfConnectedPairs}");
                    Console.WriteLine($"Number of total pairs: {(numOfLocations * (numOfLocations - 1)) / 2}");

                    adjacencyMatrix = mbBW.Dense(map.Count, map.Count, 0);
                    sumOfConnectedPairs = 0;
                }
                return adjacencyMatrix;
            }

            if (isStatStream)
            {
                foreach (var pair in resultMap)
                {
                    BasicWindowDFTResult bwdr = pair.Value;
                    double corr = bwdr.CorrBasicWindowDFT;

                    if (Math.Abs(corr) < threshold)
                    {
                        continue;
                    }
                    int key = Int32.Parse(pair.Key);
                    int m = key / 1000;
                    int j = key % 1000;
                    adjacencyMatrix[m, j] = 1;
                    adjacencyMatrix[j, m] = 1;
                    sumOfConnectedPairs += 1;
                }

                foreach (var pair in resultMap)
                {
                    BasicWindowDFTResult bwdr = pair.Value;
                    double corr = bwdr.CorrBasicWindow;

                    if (Math.Abs(corr) < threshold)
                    {
                        continue;
                    }
                    int key = Int32.Parse(pair.Key);
                    int m = key / 1000;
                    int j = key % 1000;
                    adjacencyMatrixBW[m, j] = 1;
                    adjacencyMatrixBW[j, m] = 1;
                    sumOfConnectedPairsBW += 1;
                }

                double similarity = UtilFuncs.MatrixSimilarity(adjacencyMatrix, adjacencyMatrixBW);

                Console.WriteLine($"Number of connected pairs in DFT: {sumOfConnectedPairs}");
                Console.WriteLine($"Number of connected pairs: {sumOfConnectedPairsBW}");
                Console.WriteLine($"Number of total pairs: {(numOfLocations * (numOfLocations - 1)) / 2}");
                Console.WriteLine($"Ratio: {similarity}");

                return adjacencyMatrix;
            }
            Stopwatch sw_2 = new Stopwatch();
            sw_2.Start();

            int minNum = int.MaxValue;
            foreach (var pair in resultMap)
            {
                BasicWindowDFTResult bwdr = pair.Value;
                int windowNums = size > 0 ? size / bwdr.Granularity : bwdr.ListOfCorr.Count;

                if (bwdr.ListOfCorr.Count < minNum)
                {
                    minNum = bwdr.ListOfCorr.Count;
                }

                double corr = 0.0;
                double numerator = 0.0;
                double demoninator1 = 0.0;
                double demoninator2 = 0.0;
                List<double> listOfMeanX = bwdr.ListOfMeanX.GetRange(0, windowNums);
                List<double> listOfMeanY = bwdr.ListOfMeanY.GetRange(0, windowNums);
                double meanX = listOfMeanX.Average();
                double meanY = listOfMeanY.Average();
                List<double> listOfDeltaX = new List<double>();
                List<double> listOfDeltaY = new List<double>();
                foreach (var x in listOfMeanX)
                {
                    listOfDeltaX.Add(x - meanX);
                }
                foreach (var y in listOfMeanY)
                {
                    listOfDeltaY.Add(y - meanY);
                }
                for (int i = 0; i < windowNums; i += 1)
                {
                    numerator += bwdr.ListOfSigmaX[i] * bwdr.ListOfSigmaY[i] * bwdr.ListOfDXY[i] * bwdr.ListOfDXY[i]
                                - 2 * bwdr.ListOfSigmaX[i] * bwdr.ListOfSigmaY[i] - 2 * listOfDeltaX[i] * listOfDeltaY[i];
                    if (double.IsNaN(numerator))
                    {
                        throw new ArgumentException($"sigma x: {bwdr.ListOfSigmaX[i]} sigma y: {bwdr.ListOfSigmaY[i]} " +
                            $"dxy: {bwdr.ListOfDXY[i]} delta x: {listOfDeltaX[i]} delta y: {listOfDeltaY[i]}");
                    }
                    demoninator1 += bwdr.ListOfSigmaX[i] * bwdr.ListOfSigmaX[i] + listOfDeltaX[i] * listOfDeltaX[i];
                    demoninator2 += bwdr.ListOfSigmaY[i] * bwdr.ListOfSigmaY[i] + listOfDeltaY[i] * listOfDeltaY[i];
                }
                double dSquare = 2 + numerator / (Math.Sqrt(demoninator1) * Math.Sqrt(demoninator2));
                corr = 1 - 0.5 * dSquare;

                if (corr < -1 || corr > 1)
                {
                    throw new Exception($"Abs(corr) is larger than 1, corr is {corr}");
                }

                if (Math.Abs(corr) < threshold)
                {
                    continue;
                }
                int key = Int32.Parse(pair.Key);
                int m = key / 1000;
                int j = key % 1000;
                adjacencyMatrix[m, j] = 1;
                adjacencyMatrix[j, m] = 1;
                sumOfConnectedPairs += 1;
            }
            Console.WriteLine($"minNum: {minNum}");

            Console.WriteLine($"Number of connected pairs in DFT: {sumOfConnectedPairs}");
            Console.WriteLine($"Number of total pairs: {(numOfLocations * (numOfLocations - 1)) / 2}");

            sw_2.Stop();
            Console.WriteLine($"Time used for query: {sw_2.Elapsed.ToString()}");

            return adjacencyMatrix;
        }

        public static Matrix<double> ConstructNetworkFromMapBasicWindow(
            Dictionary<string, List<Point>> map, ref Dictionary<string, BasicWindowResult> resultMapBWR,
            double threshold = 0.75, int size = -1, bool isRealtime = false, bool isTestQueryTime = false)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            // Store locations in list
            List<string> locations = new List<string>();

            // Convert to streams
            Dictionary<string, IObservableIngressStreamable<Point>> mapOfStreams =
                new Dictionary<string, IObservableIngressStreamable<Point>>();

            foreach (var pair in map)
            {
                string location = pair.Key;
                locations.Add(location);
                if (mapOfStreams.ContainsKey(location))
                {
                    throw new ArgumentException($"Duplicated Location: {{{location}}} !");
                }
                List<Point> listOfPoints = pair.Value;
                IObservable<Point> pointObservale = listOfPoints.ToObservable();
                IObservable<StreamEvent<Point>> streamEventObservable =
                    pointObservale.Select(e => StreamEvent.CreateInterval(e.Timestamp, e.Timestamp + 1, e));
                IObservableIngressStreamable<Point> ingressOberservable =
                    streamEventObservable.ToStreamable(DisorderPolicy.Drop());
                mapOfStreams.Add(location, ingressOberservable);
                //ingressOberservable.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();
            }

            Console.WriteLine(mapOfStreams.Count);

            var mb = Matrix<double>.Build;
            var adjacencyMatrix = mb.Dense(map.Count, map.Count, 0); // Symmetric Matrix
            //Console.WriteLine("Matrix: " + adjacencyMatrix.ToString());

            ConcurrentDictionary<string, BasicWindowResult>  resultMap = new ConcurrentDictionary<string, BasicWindowResult>();

            int sumOfConnectedPairs = 0;
            int numOfLocations = adjacencyMatrix.RowCount;
            Parallel.For(0, numOfLocations, i =>
            {
                for (int j = i + 1; j < numOfLocations; j += 1)
                {
                    string leftLoc = locations[i];
                    string rightLoc = locations[j];
                    IObservableIngressStreamable<Point> leftIngressOberservable = mapOfStreams[leftLoc];
                    IObservableIngressStreamable<Point> rightIngressOberservable = mapOfStreams[rightLoc];
                    // Cross join
                    var jointOutput = leftIngressOberservable.Join(
                        rightIngressOberservable,
                        (left, right) => new BasicWindowInput { X = left.Avg_temp, Y = right.Avg_temp }
                        );

                    // TODO length difference between left and right?
                    var leftLengthOfSeries = map[leftLoc].Count;
                    var rightLengthOfSeries = map[rightLoc].Count;
                    var leftDuration = map[leftLoc][leftLengthOfSeries - 1].Timestamp;
                    var rightDuration = map[rightLoc][rightLengthOfSeries - 1].Timestamp;
                    var duration = Math.Max(leftDuration, rightDuration);
                    var offset = 0;
                    var avgOutput = jointOutput.TumblingWindowLifetime(duration, offset).Aggregate(
                        w => BasicWindowExtensions.BasicWindow(w, v => v));

                    avgOutput.ToStreamEventObservable().ForEachAsync(e =>
                    {
                        if (e.IsData)
                        {
                            resultMap.TryAdd(Convert.ToString(i * 1000 + j), e.Payload); // TODO
                            //Console.WriteLine(e.Payload);
                        }
                    }).Wait();
                }
            });

            sw.Stop();
            Console.WriteLine($"Time used for joining: {sw.Elapsed.ToString()}");

            if (isRealtime)
            {
                foreach (var pair in resultMap)
                {
                    resultMapBWR.Add(pair.Key, new BasicWindowResult(pair.Value));
                }
                return adjacencyMatrix;
            }

            if (isTestQueryTime)
            {
                List<int> list = new List<int>() { 500, 1000, 1500, 2000, 2500, 3000, 3500, 4000 };
                foreach (int k in list)
                {
                    Stopwatch sw_2 = new Stopwatch();
                    sw_2.Start();

                    foreach (var pair in resultMap)
                    {
                        BasicWindowResult bwr = pair.Value;
                        int windowNums = k / bwr.Granularity;

                        double corr = 0.0;
                        double numerator = 0.0;
                        double demoninator1 = 0.0;
                        double demoninator2 = 0.0;
                        List<double> listOfMeanX = bwr.ListOfMeanX.GetRange(0, windowNums);
                        List<double> listOfMeanY = bwr.ListOfMeanY.GetRange(0, windowNums);
                        double meanX = listOfMeanX.Average();
                        double meanY = listOfMeanY.Average();
                        List<double> listOfDeltaX = new List<double>();
                        List<double> listOfDeltaY = new List<double>();
                        foreach (var x in listOfMeanX)
                        {
                            listOfDeltaX.Add(x - meanX);
                        }
                        foreach (var y in listOfMeanY)
                        {
                            listOfDeltaY.Add(y - meanY);
                        }
                        for (int i = 0; i < windowNums; i += 1)
                        {
                            numerator += bwr.ListOfSigmaX[i] * bwr.ListOfSigmaY[i] * bwr.ListOfCXY[i] + listOfDeltaX[i] * listOfDeltaY[i];
                            if (double.IsNaN(numerator))
                            {
                                throw new ArgumentException($"sigma x: {bwr.ListOfSigmaX[i]} sigma y: {bwr.ListOfSigmaY[i]} " +
                                    $"cxy: {bwr.ListOfCXY[i]} delta x: {listOfDeltaX[i]} delta y: {listOfDeltaY[i]}");
                            }
                            demoninator1 += bwr.ListOfSigmaX[i] * bwr.ListOfSigmaX[i] + listOfDeltaX[i] * listOfDeltaX[i];
                            demoninator2 += bwr.ListOfSigmaY[i] * bwr.ListOfSigmaY[i] + listOfDeltaY[i] * listOfDeltaY[i];
                        }
                        corr = numerator / (Math.Sqrt(demoninator1) * Math.Sqrt(demoninator2));

                        if (Math.Abs(corr) < threshold)
                        {
                            continue;
                        }
                        int key = Int32.Parse(pair.Key);
                        int m = key / 1000;
                        int j = key % 1000;
                        adjacencyMatrix[m, j] = 1;
                        adjacencyMatrix[j, m] = 1;
                        sumOfConnectedPairs += 1;
                    }

                    Console.WriteLine($"Number of connected pairs: {sumOfConnectedPairs}");
                    Console.WriteLine($"Number of total pairs: {(numOfLocations * (numOfLocations - 1)) / 2}");

                    sw_2.Stop();
                    Console.WriteLine($"Time used for query: {sw_2.Elapsed.ToString()}");

                    sumOfConnectedPairs = 0;
                    adjacencyMatrix = mb.Dense(map.Count, map.Count, 0);
                }
                return adjacencyMatrix;
            }

            Stopwatch sw_1 = new Stopwatch();
            sw_1.Start();

            int minNum = int.MaxValue;

            foreach (var pair in resultMap)
            {
                BasicWindowResult bwr = pair.Value;
                int windowNums = size > 0 ? size / bwr.Granularity : bwr.ListOfCXY.Count;

                if (bwr.ListOfCXY.Count < minNum)
                {
                    minNum = bwr.ListOfCXY.Count;
                }

                double corr = 0.0;
                double numerator = 0.0;
                double demoninator1 = 0.0;
                double demoninator2 = 0.0;
                List<double> listOfMeanX = bwr.ListOfMeanX.GetRange(0, windowNums);
                List<double> listOfMeanY = bwr.ListOfMeanY.GetRange(0, windowNums);
                double meanX = listOfMeanX.Average();
                double meanY = listOfMeanY.Average();
                List<double> listOfDeltaX = new List<double>();
                List<double> listOfDeltaY = new List<double>();
                foreach (var x in listOfMeanX)
                {
                    listOfDeltaX.Add(x - meanX);
                }
                foreach (var y in listOfMeanY)
                {
                    listOfDeltaY.Add(y - meanY);
                }
                for (int i = 0; i < windowNums; i += 1)
                {
                    numerator += bwr.ListOfSigmaX[i] * bwr.ListOfSigmaY[i] * bwr.ListOfCXY[i] + listOfDeltaX[i] * listOfDeltaY[i];
                    if (double.IsNaN(numerator))
                    {
                        throw new ArgumentException($"sigma x: {bwr.ListOfSigmaX[i]} sigma y: {bwr.ListOfSigmaY[i]} " +
                            $"cxy: {bwr.ListOfCXY[i]} delta x: {listOfDeltaX[i]} delta y: {listOfDeltaY[i]}");
                    }
                    demoninator1 += bwr.ListOfSigmaX[i] * bwr.ListOfSigmaX[i] + listOfDeltaX[i] * listOfDeltaX[i];
                    demoninator2 += bwr.ListOfSigmaY[i] * bwr.ListOfSigmaY[i] + listOfDeltaY[i] * listOfDeltaY[i];
                }
                corr = numerator / (Math.Sqrt(demoninator1) * Math.Sqrt(demoninator2));

                if (Math.Abs(corr) < threshold)
                {
                    continue;
                }
                int key = Int32.Parse(pair.Key);
                int m = key / 1000;
                int j = key % 1000;
                adjacencyMatrix[m, j] = 1;
                adjacencyMatrix[j, m] = 1;
                sumOfConnectedPairs += 1;
            }

            Console.WriteLine($"minNum: {minNum}");

            Console.WriteLine($"Number of connected pairs: {sumOfConnectedPairs}");
            Console.WriteLine($"Number of total pairs: {(numOfLocations * (numOfLocations - 1)) / 2}");

            sw_1.Stop();
            Console.WriteLine($"Time used for query: {sw_1.Elapsed.ToString()}");

            return adjacencyMatrix;
        }
    }

    internal class DiskDataExtraction
    {
        private readonly string dir;
        private List<StreamReader> data;
        public Dictionary<string, List<Point>> map;
        private bool getMap;

        public Dictionary<string, BasicWindowResult> resultMapBW;
        public Dictionary<string, BasicWindowDFTResult> resultMapBWDFT;

        public DiskDataExtraction(string dir)
        {
            this.dir = dir;
            data = new List<StreamReader>();
            map = new Dictionary<string, List<Point>>();
            getMap = false;
            resultMapBW = new Dictionary<string, BasicWindowResult>();
            resultMapBWDFT = new Dictionary<string, BasicWindowDFTResult>();
        }

        public void Init()
        {
            DirectoryInfo folder = new DirectoryInfo(this.dir);
            foreach (FileInfo file in folder.GetFiles("*.txt"))
            {
                try
                {
                    StreamReader sr = new StreamReader(file.FullName);
                    data.Add(sr);
                }
                catch (Exception e)
                {
                    Console.WriteLine($"The file {file.FullName} cannot be read!");
                    Console.WriteLine(e.Message);
                }
            }
        }

        public void GetMap()
        {
            foreach (StreamReader sr in data)
            {
                List<Point> list = new List<Point>();
                string line;

                var rand = new Random();

                do
                {
                    line = sr.ReadLine();
                    if (line == null)
                    {
                        break;
                    }
                    Point p = UtilFuncs.ConvertToPoint(line);
                    if (p.Avg_temp < -1000 || p.Avg_temp > 1000)
                    {
                        // TODO !!!
                        //continue;
                        p.Avg_temp = rand.NextDouble();
                        //p.Avg_temp = 0;
                    }
                    list.Add(p);
                } while (line != null);
                string location = UtilFuncs.GetLoc(list[0]);
                if (map.ContainsKey(location))
                {
                    throw new ArgumentException($"Duplicated Location: {{{location}}} !");
                }
                map.Add(location, list);
            }
            getMap = true;
        }

        public void GetResultMap(bool isBasicWindow, int queryWindowLength = -1)
        {
            if (!getMap)
            {
                GetMap();
            }

            Dictionary<string, List<Point>> tempMap = new Dictionary<string, List<Point>>();
            if (queryWindowLength > 0)
            {
                foreach (var pair in map)
                {
                    if (pair.Value.Count < queryWindowLength)
                    {
                        throw new Exception($"pair.Value.Count: {pair.Value.Count}");
                    }
                    tempMap.Add(pair.Key, new List<Point>(pair.Value.GetRange(0, queryWindowLength)));
                }
            }
            else
            {
                tempMap = new Dictionary<string, List<Point>>(map);
            }

            Stopwatch sw = new Stopwatch();
            sw.Start();
            if (isBasicWindow)
            {
                NetworkConstruction.ConstructNetworkFromMapBasicWindow(tempMap, ref resultMapBW, isRealtime: true);
            }
            else
            {
                NetworkConstruction.ConstructNetworkFromMapBasicWindowDFT(tempMap, ref resultMapBWDFT, isRealtime: true);
            }
            sw.Stop();
            Console.WriteLine($"Time used for network construction: {sw.Elapsed.ToString()}");
        }

        public void RealTimeSimulationTSUBASA(Dictionary<string, BasicWindowResult> newBWMap)
        {
            Dictionary<string, double> memoCorr = new Dictionary<string, double>();

            int numOfLocations = map.Count;
            int sumOfConnectedPairs = 0;
            var mb = Matrix<double>.Build;
            var adjacencyMatrix = mb.Dense(map.Count, map.Count, 0); // Symmetric Matrix

            int windowNums = 0;
            foreach (var pair in resultMapBW)
            {
                var key = pair.Key;
                var bwr = pair.Value;

                windowNums = bwr.ListOfCXY.Count;
                double corr = 0.0;
                double numerator = 0.0;
                double demoninator1 = 0.0;
                double demoninator2 = 0.0;
                List<double> listOfMeanX = bwr.ListOfMeanX.GetRange(0, windowNums);
                List<double> listOfMeanY = bwr.ListOfMeanY.GetRange(0, windowNums);
                double meanX = listOfMeanX.Average();
                double meanY = listOfMeanY.Average();
                List<double> listOfDeltaX = new List<double>();
                List<double> listOfDeltaY = new List<double>();
                foreach (var x in listOfMeanX)
                {
                    listOfDeltaX.Add(x - meanX);
                }
                foreach (var y in listOfMeanY)
                {
                    listOfDeltaY.Add(y - meanY);
                }
                for (int i = 0; i < windowNums; i += 1)
                {
                    numerator += bwr.ListOfSigmaX[i] * bwr.ListOfSigmaY[i] * bwr.ListOfCXY[i] + listOfDeltaX[i] * listOfDeltaY[i];
                    if (double.IsNaN(numerator))
                    {
                        throw new ArgumentException($"sigma x: {bwr.ListOfSigmaX[i]} sigma y: {bwr.ListOfSigmaY[i]} " +
                            $"cxy: {bwr.ListOfCXY[i]} delta x: {listOfDeltaX[i]} delta y: {listOfDeltaY[i]}");
                    }
                    demoninator1 += bwr.ListOfSigmaX[i] * bwr.ListOfSigmaX[i] + listOfDeltaX[i] * listOfDeltaX[i];
                    demoninator2 += bwr.ListOfSigmaY[i] * bwr.ListOfSigmaY[i] + listOfDeltaY[i] * listOfDeltaY[i];
                }
                corr = numerator / (Math.Sqrt(demoninator1) * Math.Sqrt(demoninator2));
                memoCorr.Add(key, corr);
            }


            Stopwatch sw = new Stopwatch();
            sw.Start();
            windowNums = 0;
            foreach (var pair in resultMapBW)
            {
                var key = pair.Key;
                var bwr = pair.Value;
                if (!newBWMap.ContainsKey(key))
                {
                    Console.WriteLine($"Cannot find {key}");
                    continue;
                }
                var newBwr = newBWMap[key];

                windowNums = bwr.ListOfCXY.Count;
                double corr = memoCorr[key];

                List<double> listOfMeanX = bwr.ListOfMeanX.GetRange(0, windowNums);
                List<double> listOfMeanY = bwr.ListOfMeanY.GetRange(0, windowNums);
                double meanX = listOfMeanX.Average();
                double meanY = listOfMeanY.Average();
                List<double> listOfDeltaX = new List<double>();
                List<double> listOfDeltaY = new List<double>();
                foreach (var x in listOfMeanX)
                {
                    listOfDeltaX.Add(x - meanX);
                    break;
                }
                foreach (var y in listOfMeanY)
                {
                    listOfDeltaY.Add(y - meanY);
                    break;
                }

                double alphaX = (newBwr.ListOfMeanX[0] - bwr.ListOfMeanX[0]) / windowNums;
                double alphaY = (newBwr.ListOfMeanY[0] - bwr.ListOfMeanY[0]) / windowNums;

                double deltaXNew = newBwr.ListOfMeanX[0] - meanX;
                double deltaYNew = newBwr.ListOfMeanY[0] - meanY;

                double cNew = newBwr.ListOfCXY[0];

                double A = Math.Sqrt(windowNums * bwr.StdX * bwr.StdX - bwr.ListOfSigmaX[0] * bwr.ListOfSigmaX[0] - listOfDeltaX[0] * listOfDeltaX[0]
                    + newBwr.ListOfSigmaX[0] * newBwr.ListOfSigmaX[0] - windowNums * alphaX * alphaX + deltaXNew * deltaXNew);
                double B = Math.Sqrt(windowNums * bwr.StdY * bwr.StdY - bwr.ListOfSigmaY[0] * bwr.ListOfSigmaY[0]
                    + newBwr.ListOfSigmaY[0] * newBwr.ListOfSigmaY[0] - windowNums * alphaY * alphaY + deltaYNew * deltaYNew);

                double corrNew = (windowNums * bwr.StdX * bwr.StdY * corr + newBwr.ListOfSigmaX[0] * newBwr.ListOfSigmaY[0] * cNew
                    - bwr.ListOfSigmaX[0] * bwr.ListOfSigmaY[0] * bwr.ListOfCXY[0] - listOfDeltaX[0] * listOfDeltaY[0] - windowNums * alphaX * alphaY
                    + deltaXNew * deltaYNew) / (A * B);

                if (Math.Abs(corrNew) < 0.75)
                {
                    continue;
                }

                int m = Int32.Parse(key) / 1000;
                int j = Int32.Parse(key) % 1000;
                adjacencyMatrix[m, j] = 1;
                adjacencyMatrix[j, m] = 1;
                sumOfConnectedPairs += 1;

            }
            sw.Stop();
            Console.WriteLine($"Time used for real time network upadte: {sw.Elapsed.ToString()}");

            Console.WriteLine($"Number of connected pairs: {sumOfConnectedPairs}");
            Console.WriteLine($"Number of total pairs: {(numOfLocations * (numOfLocations - 1))}");
        }

        public void RealTimeSimulationTSUBASADFT(Dictionary<string, BasicWindowDFTResult> newBWDFTMap)
        {
            Dictionary<string, double> memoCorr = new Dictionary<string, double>();

            int numOfLocations = map.Count;
            int sumOfConnectedPairs = 0;
            var mb = Matrix<double>.Build;
            var adjacencyMatrix = mb.Dense(map.Count, map.Count, 0); // Symmetric Matrix

            int windowNums = 0;
            foreach (var pair in resultMapBWDFT)
            {
                string key = pair.Key;
                BasicWindowDFTResult bwdr = pair.Value;
                windowNums = bwdr.ListOfCorr.Count;

                double corr = 0.0;
                double numerator = 0.0;
                double demoninator1 = 0.0;
                double demoninator2 = 0.0;
                List<double> listOfMeanX = bwdr.ListOfMeanX.GetRange(0, windowNums);
                List<double> listOfMeanY = bwdr.ListOfMeanY.GetRange(0, windowNums);
                double meanX = listOfMeanX.Average();
                double meanY = listOfMeanY.Average();
                List<double> listOfDeltaX = new List<double>();
                List<double> listOfDeltaY = new List<double>();
                foreach (var x in listOfMeanX)
                {
                    listOfDeltaX.Add(x - meanX);
                }
                foreach (var y in listOfMeanY)
                {
                    listOfDeltaY.Add(y - meanY);
                }
                for (int i = 0; i < windowNums; i += 1)
                {
                    numerator += bwdr.ListOfSigmaX[i] * bwdr.ListOfSigmaY[i] * bwdr.ListOfDXY[i] * bwdr.ListOfDXY[i]
                                - 2 * bwdr.ListOfSigmaX[i] * bwdr.ListOfSigmaY[i] - 2 * listOfDeltaX[i] * listOfDeltaY[i];
                    if (double.IsNaN(numerator))
                    {
                        throw new ArgumentException($"sigma x: {bwdr.ListOfSigmaX[i]} sigma y: {bwdr.ListOfSigmaY[i]} " +
                            $"dxy: {bwdr.ListOfDXY[i]} delta x: {listOfDeltaX[i]} delta y: {listOfDeltaY[i]}");
                    }
                    demoninator1 += bwdr.ListOfSigmaX[i] * bwdr.ListOfSigmaX[i] + listOfDeltaX[i] * listOfDeltaX[i];
                    demoninator2 += bwdr.ListOfSigmaY[i] * bwdr.ListOfSigmaY[i] + listOfDeltaY[i] * listOfDeltaY[i];
                }
                double dSquare = 2 + numerator / (Math.Sqrt(demoninator1) * Math.Sqrt(demoninator2));
                corr = 1 - 0.5 * dSquare;

                if (corr < -1 || corr > 1)
                {
                    throw new Exception($"Abs(corr) is larger than 1, corr is {corr}");
                }

                memoCorr.Add(key, corr);
            }

            Stopwatch sw = new Stopwatch();
            sw.Start();
            windowNums = 0;
            foreach (var pair in resultMapBWDFT)
            {
                var key = pair.Key;
                var bwrd = pair.Value;
                if (!newBWDFTMap.ContainsKey(key))
                {
                    Console.WriteLine($"Cannot find {key}");
                    continue;
                }
                var newBwrd = newBWDFTMap[key];

                windowNums = bwrd.ListOfDXY.Count;
                double corr = memoCorr[key];

                List<double> listOfMeanX = bwrd.ListOfMeanX.GetRange(0, windowNums);
                List<double> listOfMeanY = bwrd.ListOfMeanY.GetRange(0, windowNums);
                double meanX = listOfMeanX.Average();
                double meanY = listOfMeanY.Average();
                List<double> listOfDeltaX = new List<double>();
                List<double> listOfDeltaY = new List<double>();
                foreach (var x in listOfMeanX)
                {
                    listOfDeltaX.Add(x - meanX);
                    break;
                }
                foreach (var y in listOfMeanY)
                {
                    listOfDeltaY.Add(y - meanY);
                    break;
                }

                double alphaX = (newBwrd.ListOfMeanX[0] - bwrd.ListOfMeanX[0]) / windowNums;
                double alphaY = (newBwrd.ListOfMeanY[0] - bwrd.ListOfMeanY[0]) / windowNums;

                double deltaXNew = newBwrd.ListOfMeanX[0] - meanX;
                double deltaYNew = newBwrd.ListOfMeanY[0] - meanY;

                double dNew = newBwrd.ListOfDXY[0];
                double cNew = 1 - 0.5 * dNew * dNew;

                double A = Math.Sqrt(windowNums * bwrd.StdX * bwrd.StdX - bwrd.ListOfSigmaX[0] * bwrd.ListOfSigmaX[0] - listOfDeltaX[0] * listOfDeltaX[0]
                    + newBwrd.ListOfSigmaX[0] * newBwrd.ListOfSigmaX[0] - windowNums * alphaX * alphaX + deltaXNew * deltaXNew);
                double B = Math.Sqrt(windowNums * bwrd.StdY * bwrd.StdY - bwrd.ListOfSigmaY[0] * bwrd.ListOfSigmaY[0]
                    + newBwrd.ListOfSigmaY[0] * newBwrd.ListOfSigmaY[0] - windowNums * alphaY * alphaY + deltaYNew * deltaYNew);

                double corrNew = (windowNums * bwrd.StdX * bwrd.StdY * corr + newBwrd.ListOfSigmaX[0] * newBwrd.ListOfSigmaY[0] * cNew
                    - bwrd.ListOfSigmaX[0] * bwrd.ListOfSigmaY[0] * (1 - 0.5 * bwrd.ListOfDXY[0] * bwrd.ListOfDXY[0])
                    - listOfDeltaX[0] * listOfDeltaY[0] - windowNums * alphaX * alphaY + deltaXNew * deltaYNew) / (A * B);

                if (Math.Abs(corrNew) < 0.75)
                {
                    continue;
                }

                int m = Int32.Parse(key) / 1000;
                int j = Int32.Parse(key) % 1000;
                adjacencyMatrix[m, j] = 1;
                adjacencyMatrix[j, m] = 1;
                sumOfConnectedPairs += 1;

            }
            sw.Stop();
            Console.WriteLine($"Time used for real time network upadte: {sw.Elapsed.ToString()}");

            Console.WriteLine($"Number of connected pairs: {sumOfConnectedPairs}");
            Console.WriteLine($"Number of total pairs: {(numOfLocations * (numOfLocations - 1))}");
        }

        public Matrix<double> ConstructNetwork(int queryWindowLength = -1)
        {
            if (!getMap)
            {
                GetMap();
            }

            Dictionary<string, List<Point>> tempMap = new Dictionary<string, List<Point>>();
            if (queryWindowLength > 0)
            {
                foreach (var pair in map)
                {
                    if (pair.Value.Count < queryWindowLength)
                    {
                        throw new Exception($"pair.Value.Count: {pair.Value.Count}");
                    }
                    tempMap.Add(pair.Key, new List<Point>(pair.Value.GetRange(0, queryWindowLength)));
                }
            }
            else
            {
                tempMap = new Dictionary<string, List<Point>>(map);
            }

            Stopwatch sw = new Stopwatch();
            sw.Start();
            Matrix<double> matrix = NetworkConstruction.ConstructNetworkFromMap(tempMap);
            sw.Stop();
            Console.WriteLine($"Time used for network construction: {sw.Elapsed.ToString()}");
            return matrix;
        }

        public Matrix<double> ConstructNetworkBasicWindow(int queryWindowLength = -1, int length = -1, bool isTestForQueryTime = false)
        {
            if (!getMap)
            {
                GetMap();
            }

            Dictionary<string, List<Point>> tempMap = new Dictionary<string, List<Point>>();
            if (queryWindowLength > 0)
            {
                foreach (var pair in map)
                {
                    if (pair.Value.Count < queryWindowLength)
                    {
                        throw new Exception($"pair.Value.Count: {pair.Value.Count}");
                    }
                    tempMap.Add(pair.Key, new List<Point>(pair.Value.GetRange(0, queryWindowLength)));
                }
            }
            else
            {
                tempMap = new Dictionary<string, List<Point>>(map);
            }
            Stopwatch sw = new Stopwatch();
            sw.Start();
            Matrix<double> matrix = NetworkConstruction.ConstructNetworkFromMapBasicWindow(tempMap, ref resultMapBW, isRealtime: false, size: length, isTestQueryTime: isTestForQueryTime);
            sw.Stop();
            Console.WriteLine($"Time used for network construction: {sw.Elapsed.ToString()}");
            return matrix;
        }

        public Matrix<double> ConstructNetworkBasicWindowDFT(int queryWindowLength = -1, int length = -1, bool isTestForQueryTime = false, bool useStatStream = false)
        {
            if (!getMap)
            {
                GetMap();
            }

            Dictionary<string, List<Point>> tempMap = new Dictionary<string, List<Point>>();
            if (queryWindowLength > 0)
            {
                foreach (var pair in map)
                {
                    if (pair.Value.Count < queryWindowLength)
                    {
                        throw new Exception($"pair.Value.Count: {pair.Value.Count}");
                    }
                    tempMap.Add(pair.Key, new List<Point>(pair.Value.GetRange(0, queryWindowLength)));
                }
            }
            else
            {
                tempMap = new Dictionary<string, List<Point>>(map);
            }

            Stopwatch sw = new Stopwatch();
            sw.Start();
            Matrix<double> matrix = NetworkConstruction.ConstructNetworkFromMapBasicWindowDFT(tempMap, ref resultMapBWDFT,
                size: length, isTestQueryTime: isTestForQueryTime, isStatStream: useStatStream);
            sw.Stop();
            Console.WriteLine($"Time used for network construction: {sw.Elapsed.ToString()}");
            return matrix;
        }

        public void Clear()
        {
            foreach (StreamReader sr in data)
            {
                sr.Close();
            }
            data.Clear();
            map.Clear();
            getMap = false;
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            const string dir = @"/Users/xxx/xxx/xxx";
            DiskDataExtraction dde = new DiskDataExtraction(dir);
            dde.Init();
            dde.ConstructNetworkBasicWindowDFT(queryWindowLength: 8000); 
            //dde.ConstructNetwork();
            //dde.ConstructNetworkBasicWindow(isTestForQueryTime: true); 

            //dde.GetResultMap(false, queryWindowLength: 4000);
            //Console.WriteLine(dde.resultMapBWDFT.Count);

            //DiskDataExtraction dde1 = new DiskDataExtraction(dir);
            //dde1.Init();
            //dde1.GetResultMap(false, queryWindowLength: 10);
            //Console.WriteLine(dde1.resultMapBWDFT.Count);
            //dde.RealTimeSimulationTSUBASADFT(dde1.resultMapBWDFT);
            //dde1.Clear();

            dde.Clear();
        }
    }
}
